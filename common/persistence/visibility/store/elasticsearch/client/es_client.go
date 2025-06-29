package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/elastic/go-elasticsearch/v9/esutil"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
)

type (
	ESClient struct {
		ESClient                   *elasticsearch.Client
		url                        url.URL
		initIsPointInTimeSupported sync.Once
		isPointInTimeSupported     bool
		Healthcheck                Healthcheck
	}

	Healthcheck struct {
		healthcheckEnabled        bool
		healthcheckTimeoutStartup time.Duration
		healthcheckTimeout        time.Duration
		healthcheckInterval       time.Duration
		healthcheckStopChan       chan bool
	}
)

const (
	pointInTimeSupportedFlavor   = "default" // the other flavor is "oss"
	DefaultDiscoverNodesInterval = 15 * time.Minute
)

var (
	pointInTimeSupportedIn = semver.MustParseRange(">=7.10.0")
)

var _ Client = (*ESClient)(nil)

func NewESClient(cfg *Config, httpClient *http.Client, logger log.Logger) (*ESClient, error) {
	var urls []string
	if len(cfg.URLs) > 0 {
		urls = make([]string, len(cfg.URLs))
		for i, u := range cfg.URLs {
			urls[i] = u.String()
		}
	} else {
		urls = []string{cfg.URL.String()}
	}

	if httpClient == nil {
		if cfg.TLS != nil && cfg.TLS.Enabled {
			tlsHttpClient, err := buildTLSHTTPClient(cfg.TLS)
			if err != nil {
				return nil, fmt.Errorf("failed to create TLS config: %w", err)
			}
			httpClient = tlsHttpClient
		} else {
			httpClient = http.DefaultClient
		}
	}

	esCfg := elasticsearch.Config{
		Addresses:                urls,
		Username:                 cfg.Username,
		Password:                 cfg.Password,
		CompressRequestBody:      true,
		CompressRequestBodyLevel: gzip.DefaultCompression,
		Transport:                httpClient.Transport,
		EnableDebugLogger:        true,
		EnableMetrics:            true,
		DiscoverNodesOnStart:     cfg.EnableSniff,
		DiscoverNodesInterval:    DefaultDiscoverNodesInterval,
		// RetryBackoff:             func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		// MaxRetries:               5,
		// RetryOnStatus:            []int{429, 502, 503, 504},
	}

	if cfg.CloseIdleConnectionsInterval != time.Duration(0) {
		if cfg.CloseIdleConnectionsInterval < minimumCloseIdleConnectionsInterval {
			cfg.CloseIdleConnectionsInterval = minimumCloseIdleConnectionsInterval
		}
		go func(interval time.Duration, httpClient *http.Client) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				httpClient.CloseIdleConnections()
			}
		}(cfg.CloseIdleConnectionsInterval, httpClient)
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, err
	}

	return &ESClient{
		ESClient: client,
		url:      cfg.URL,
	}, nil
}

// Build Http Client with TLS
func buildTLSHTTPClient(config *auth.TLS) (*http.Client, error) {
	tlsConfig, err := auth.NewTLSConfig(config)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	tlsClient := &http.Client{Transport: transport}

	return tlsClient, nil
}

func (c *ESClient) Get(ctx context.Context, index string, docID string) (*GetResult, error) {
	req := esapi.GetRequest{
		Index:      index,
		DocumentID: docID,
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("error getting document %s: %s", docID, res.String())
	}

	var getResult GetResult
	if err := json.NewDecoder(res.Body).Decode(&getResult); err != nil {
		return nil, fmt.Errorf("error decoding response body: %w", err)
	}
	return &getResult, nil
}

func (c *ESClient) Search(ctx context.Context, p *SearchParametersNew) (*SearchResult, error) {
	query := map[string]interface{}{
		"query":            p.Query,
		"sort":             p.Sorter,
		"track_total_hits": false,
	}

	if p.PageSize != 0 {
		query["size"] = p.PageSize
	}
	if len(p.SearchAfter) > 0 {
		query["search_after"] = p.SearchAfter
	}
	if p.PointInTime != nil {
		query["point_in_time"] = map[string]interface{}{
			"id":         p.PointInTime,
			"keep_alive": "1m",
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding search query: %w", err)
	}

	req := c.ESClient.Search
	opts := []func(*esapi.SearchRequest){
		req.WithContext(ctx),
		req.WithBody(&buf),
		req.WithTrackTotalHits(false),
	}

	if p.PointInTime == nil {
		opts = append(opts, req.WithIndex(p.Index))
	}

	res, err := req(opts...)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("ES search error: %s", res.String())
	}

	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding search response: %w", err)
	}
	return &result, nil
}

// FIX: keepAliveInterval
func (c *ESClient) OpenScroll(ctx context.Context, p *SearchParametersNew, keepAliveInterval time.Duration) (*SearchResult, error) {
	query := map[string]interface{}{
		"query": p.Query,
		"sort":  p.Sorter,
	}

	if p.PageSize != 0 {
		query["size"] = p.PageSize
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding scroll query: %w", err)
	}

	res, err := c.ESClient.Search(
		c.ESClient.Search.WithContext(ctx),
		c.ESClient.Search.WithIndex(p.Index),
		c.ESClient.Search.WithBody(&buf),
		c.ESClient.Search.WithScroll(keepAliveInterval),
	)
	if err != nil {
		return nil, fmt.Errorf("error initiating scroll: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("scroll search error: %s", res.String())
	}

	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding scroll response: %w", err)
	}

	return &result, nil
}

func (c *ESClient) Scroll(ctx context.Context, scrollID string, keepAliveInterval time.Duration) (*SearchResult, error) {
	req := esapi.ScrollRequest{
		ScrollID: scrollID,
		Scroll:   keepAliveInterval,
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return nil, fmt.Errorf("error executing scroll request: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("scroll error: %s", res.String())
	}

	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding scroll response: %w", err)
	}
	return &result, nil
}

func (c *ESClient) CloseScroll(ctx context.Context, id string) error {
	req := esapi.ClearScrollRequest{
		ScrollID: []string{id},
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return fmt.Errorf("error closing scroll: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("error response from Elasticsearch when closing scroll: %s", res.String())
	}
	return nil
}

func (c *ESClient) IsPointInTimeSupported(ctx context.Context) bool {
	c.initIsPointInTimeSupported.Do(func() {
		c.isPointInTimeSupported = c.queryPointInTimeSupported(ctx)
	})
	return c.isPointInTimeSupported
}

func (c *ESClient) queryPointInTimeSupported(ctx context.Context) bool {
	req := esapi.InfoRequest{}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	if res.IsError() {
		return false
	}
	var info struct {
		Version struct {
			Number      string `json:"number"`
			BuildFlavor string `json:"build_flavor"`
		} `json:"version"`
	}
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return false
	}

	if info.Version.BuildFlavor != pointInTimeSupportedFlavor {
		return false
	}

	esVersion, err := semver.ParseTolerant(info.Version.Number)
	if err != nil {
		return false
	}
	return pointInTimeSupportedIn(esVersion)
}

func (c *ESClient) Count(ctx context.Context, index string, query map[string]interface{}) (int64, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]interface{}{
		"query": query,
	}); err != nil {
		return 0, fmt.Errorf("failed to encode query: %w", err)
	}
	req := esapi.CountRequest{
		Index: []string{index},
		Body:  &buf,
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return 0, err
	}
	if res.IsError() {
		return 0, fmt.Errorf("error counting documents in index %s: %s", index, res.String())
	}
	defer res.Body.Close()
	var r struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return 0, err
	}
	return r.Count, nil
}

func (c *ESClient) CountGroupBy(
	ctx context.Context,
	index string,
	query map[string]interface{},
	aggName string,
	agg map[string]interface{},
) (*map[string]interface{}, error) {
	searchBody := map[string]interface{}{
		"query":            query,
		"size":             0,
		"track_total_hits": false,
		"aggs": map[string]interface{}{
			aggName: agg,
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchBody); err != nil {
		return nil, fmt.Errorf("error encoding search body: %w", err)
	}

	// Send the search request
	res, err := c.ESClient.Search(
		c.ESClient.Search.WithContext(ctx),
		c.ESClient.Search.WithIndex(index),
		c.ESClient.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("ES error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	// aggs, ok := result["aggregations"].(map[string]interface{})
	// if !ok {
	// 	return nil, fmt.Errorf("missing 'aggregations' in response")
	// }
	return &result, nil
}

func (c *ESClient) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	req := esapi.IndicesGetMappingRequest{
		Index: []string{index},
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("error getting mapping for index %s: %s", index, res.String())
	}

	var body map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return convertMappingBody(body, index), nil
}

func (c *ESClient) GetDateFieldType() string {
	return "date_nanos"
}

// TODO: IMPLEMENT
func (c *ESClient) Bulk() BulkServiceN {
	// return newBulkService_n(c.ESClient)
	return nil
}

func (c *ESClient) RunBulkProcessor(ctx context.Context, p *BulkIndexerParameters) (BulkIndexer, error) {
	return newBulkProcessor_n(ctx, esutil.BulkIndexerConfig{
		Client:        c.ESClient,
		NumWorkers:    p.NumOfWorkers,
		FlushInterval: p.FlushInterval,
		OnFlushStart:  p.BeforeFunc,
		OnFlushEnd:    p.AfterFunc,
		FlushBytes:    p.BulkSize,
	})
}

func (c *ESClient) Delete(ctx context.Context, index string, docID string, version int64) error {
	versions := int(version)
	req := esapi.DeleteRequest{
		Index:      index,
		DocumentID: docID,
		Version:    &versions,
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error deleting document %s: %s", docID, res.String())
	}
	return nil
}

func (c *ESClient) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, err
	}
	if res.IsError() {
		return false, fmt.Errorf("error deleting index %s: %s", indexName, res.String())
	}
	return true, nil
}

func (c *ESClient) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return false, fmt.Errorf("error encoding index body: %w", err)
	}

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  &buf,
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, err
	}
	if res.IsError() {
		return false, fmt.Errorf("error creating index %s: %s", index, res.String())
	}
	return true, nil
}

func (c *ESClient) CatIndices(ctx context.Context, target string) (CatIndicesResponse, error) {
	req := esapi.CatIndicesRequest{
		Index:  []string{target},
		Format: "json",
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("error getting cat indices for target %s: %s", target, res.String())
	}

	var data CatIndicesResponse
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode cat indices response: %w", err)
	}

	return data, nil
}

func (c *ESClient) IsNotFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "404")
}

func (c *ESClient) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	body := buildMappingBody(mapping)
	bodybytes, err := json.Marshal(body)
	if err != nil {
		return false, fmt.Errorf("failed to marshal mapping body: %w", err)
	}
	req := esapi.IndicesPutMappingRequest{
		Index: []string{index},
		Body:  bytes.NewReader(bodybytes),
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, fmt.Errorf("failed to put mapping: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return false, fmt.Errorf("elasticsearch error: %s", res.String())
	}

	var resp struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp.Acknowledged, nil
}

func (c *ESClient) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	req := esapi.ClusterHealthRequest{
		Index:         []string{index},
		WaitForStatus: "yellow",
		Timeout:       30 * time.Second, // CHECK
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return "", fmt.Errorf("cluster health request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("elasticsearch error: %s", res.String())
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("failed to parse cluster health response: %w", err)
	}

	return body.Status, nil
}

func (c *ESClient) IndexPutTemplate(ctx context.Context, templateName, bodyString string) (bool, error) {
	req := esapi.IndicesPutTemplateRequest{
		Name: templateName,
		Body: strings.NewReader(bodyString),
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, fmt.Errorf("error putting index template %s: %w", templateName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, fmt.Errorf("error response from Elasticsearch when putting index template %s: %s", templateName, res.String())
	}

	var resp struct {
		Acknowledged bool `json:"acknowledged"`
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return false, fmt.Errorf("error decoding response body: %w", err)
	}
	return resp.Acknowledged, nil
}

func (c *ESClient) IndexPutSettings(ctx context.Context, indexName, bodyString string) (bool, error) {
	req := esapi.IndicesPutSettingsRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(bodyString),
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, fmt.Errorf("error putting index settings for %s: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, fmt.Errorf("error response from Elasticsearch when putting index settings for %s: %s", indexName, res.String())
	}

	var resp struct {
		Acknowledged bool `json:"acknowledged"`
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return false, fmt.Errorf("error decoding response body: %w", err)
	}

	return resp.Acknowledged, nil
}

func (c *ESClient) IndexExists(ctx context.Context, indexName string) (bool, error) {
	req := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, fmt.Errorf("error checking if index %s exists: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		return true, nil
	}
	if res.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("unexpected status code when checking index %s: %d", indexName, res.StatusCode)
}

func (c *ESClient) IndexGetSettings(ctx context.Context, indexName string) (map[string]*IndicesGetSettingsResponse, error) {
	req := esapi.IndicesGetSettingsRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return nil, fmt.Errorf("error getting index settings for %s: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch when getting index settings for %s: %s", indexName, res.String())
	}

	var settings map[string]*IndicesGetSettingsResponse
	if err := json.NewDecoder(res.Body).Decode(&settings); err != nil {
		return nil, fmt.Errorf("error decoding response body: %w", err)
	}

	return settings, nil
}

func (c *ESClient) Ping(ctx context.Context) error {
	req := esapi.PingRequest{}
	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch returned error: %s", res.String())
	}
	return nil
}

func (c *ESClient) OpenPointInTime(ctx context.Context, index string, keepAliveInterval time.Duration) (string, error) {
	req := esapi.OpenPointInTimeRequest{
		Index:     []string{index},
		// KeepAlive: keepAliveInterval,
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.IsError() {
		return "", fmt.Errorf("error opening point in time for index %s: %s", index, res.String())
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("failed to decode PIT response: %w", err)
	}

	return body.ID, nil
}

func (c *ESClient) ClosePointInTime(ctx context.Context, id string) (bool, error) {
	body := strings.NewReader(fmt.Sprintf(`{"id": "%s"}`, id))
	req := esapi.ClosePointInTimeRequest{
		Body: body,
	}

	res, err := req.Do(ctx, c.ESClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, fmt.Errorf("error closing point in time %s: %s", id, res.String())
	}
	return true, nil
}
