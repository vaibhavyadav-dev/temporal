//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	versionTypeExternal                 = "external"
	minimumCloseIdleConnectionsInterval = 15 * time.Second
)

type (
	// Client is a wrapper around Elasticsearch client library.
	Client interface {
		Get(ctx context.Context, index string, docID string) (*GetResult, error)
		Search(ctx context.Context, p *SearchParametersNew) (*SearchResult, error)
		Count(ctx context.Context, index string, query map[string]interface{}) (int64, error)
		CountGroupBy(ctx context.Context, index string, query map[string]interface{}, aggName string, agg map[string]interface{}) (*map[string]interface{}, error)
		RunBulkProcessor(ctx context.Context, p *BulkIndexerParameters) (BulkIndexer, error)

		// TODO (alex): move this to some admin client (and join with IntegrationTestsClient)
		PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error)
		WaitForYellowStatus(ctx context.Context, index string) (string, error)
		GetMapping(ctx context.Context, index string) (map[string]string, error)
		IndexExists(ctx context.Context, indexName string) (bool, error)
		CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		CatIndices(ctx context.Context, target string) (CatIndicesResponse, error)

		OpenScroll(ctx context.Context, p *SearchParametersNew, keepAliveInterval time.Duration) (*SearchResult, error)
		Scroll(ctx context.Context, id string, keepAliveInterval time.Duration) (*SearchResult, error)
		CloseScroll(ctx context.Context, id string) error

		IsPointInTimeSupported(ctx context.Context) bool
		OpenPointInTime(ctx context.Context, index string, keepAliveInterval time.Duration) (string, error)
		ClosePointInTime(ctx context.Context, id string) (bool, error)
	}

	CLIClient interface {
		Client
		Delete(ctx context.Context, indexName string, docID string, version int64) error
	}

	IntegrationTestsClient interface {
		Client
		IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error)
		IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error)
		IndexGetSettings(ctx context.Context, indexName string) (map[string]*IndicesGetSettingsResponse, error)
		Ping(ctx context.Context) error
	}

	// SearchParameters holds all required and optional parameters for executing a search.
	SearchParameters struct {
		Index    string
		Query    elastic.Query
		PageSize int
		Sorter   []elastic.Sorter

		SearchAfter []interface{}
		ScrollID    string
		PointInTime *elastic.PointInTime
	}

	SearchParametersNew struct {
		Index       string
		Query       map[string]interface{}
		PageSize    int
		Sorter      []map[string]interface{}
		SearchAfter []interface{}
		ScrollID    string
		PointInTime *esapi.OpenPointInTimeRequest
	}
)

type (
	IndicesGetSettingsResponse struct {
		Settings map[string]interface{} `json:"settings"`
	}

	CatIndicesResponse    []CatIndicesResponseRow
	CatIndicesResponseRow struct {
		Health                       string `json:"health"`                              // "green", "yellow", or "red"
		Status                       string `json:"status"`                              // "open" or "closed"
		Index                        string `json:"index"`                               // index name
		UUID                         string `json:"uuid"`                                // index uuid
		Pri                          int    `json:"pri,string"`                          // number of primary shards
		Rep                          int    `json:"rep,string"`                          // number of replica shards
		DocsCount                    int    `json:"docs.count,string"`                   // number of available documents
		DocsDeleted                  int    `json:"docs.deleted,string"`                 // number of deleted documents
		CreationDate                 int64  `json:"creation.date,string"`                // index creation date (millisecond value), e.g. 1527077221644
		CreationDateString           string `json:"creation.date.string"`                // index creation date (as string), e.g. "2018-05-23T12:07:01.644Z"
		StoreSize                    string `json:"store.size"`                          // store size of primaries & replicas, e.g. "4.6kb"
		PriStoreSize                 string `json:"pri.store.size"`                      // store size of primaries, e.g. "230b"
		CompletionSize               string `json:"completion.size"`                     // size of completion on primaries & replicas
		PriCompletionSize            string `json:"pri.completion.size"`                 // size of completion on primaries
		FielddataMemorySize          string `json:"fielddata.memory_size"`               // used fielddata cache on primaries & replicas
		PriFielddataMemorySize       string `json:"pri.fielddata.memory_size"`           // used fielddata cache on primaries
		FielddataEvictions           int    `json:"fielddata.evictions,string"`          // fielddata evictions on primaries & replicas
		PriFielddataEvictions        int    `json:"pri.fielddata.evictions,string"`      // fielddata evictions on primaries
		QueryCacheMemorySize         string `json:"query_cache.memory_size"`             // used query cache on primaries & replicas
		PriQueryCacheMemorySize      string `json:"pri.query_cache.memory_size"`         // used query cache on primaries
		QueryCacheEvictions          int    `json:"query_cache.evictions,string"`        // query cache evictions on primaries & replicas
		PriQueryCacheEvictions       int    `json:"pri.query_cache.evictions,string"`    // query cache evictions on primaries
		RequestCacheMemorySize       string `json:"request_cache.memory_size"`           // used request cache on primaries & replicas
		PriRequestCacheMemorySize    string `json:"pri.request_cache.memory_size"`       // used request cache on primaries
		RequestCacheEvictions        int    `json:"request_cache.evictions,string"`      // request cache evictions on primaries & replicas
		PriRequestCacheEvictions     int    `json:"pri.request_cache.evictions,string"`  // request cache evictions on primaries
		RequestCacheHitCount         int    `json:"request_cache.hit_count,string"`      // request cache hit count on primaries & replicas
		PriRequestCacheHitCount      int    `json:"pri.request_cache.hit_count,string"`  // request cache hit count on primaries
		RequestCacheMissCount        int    `json:"request_cache.miss_count,string"`     // request cache miss count on primaries & replicas
		PriRequestCacheMissCount     int    `json:"pri.request_cache.miss_count,string"` // request cache miss count on primaries
		FlushTotal                   int    `json:"flush.total,string"`                  // number of flushes on primaries & replicas
		PriFlushTotal                int    `json:"pri.flush.total,string"`              // number of flushes on primaries
		FlushTotalTime               string `json:"flush.total_time"`                    // time spent in flush on primaries & replicas
		PriFlushTotalTime            string `json:"pri.flush.total_time"`                // time spent in flush on primaries
		GetCurrent                   int    `json:"get.current,string"`                  // number of current get ops on primaries & replicas
		PriGetCurrent                int    `json:"pri.get.current,string"`              // number of current get ops on primaries
		GetTime                      string `json:"get.time"`                            // time spent in get on primaries & replicas
		PriGetTime                   string `json:"pri.get.time"`                        // time spent in get on primaries
		GetTotal                     int    `json:"get.total,string"`                    // number of get ops on primaries & replicas
		PriGetTotal                  int    `json:"pri.get.total,string"`                // number of get ops on primaries
		GetExistsTime                string `json:"get.exists_time"`                     // time spent in successful gets on primaries & replicas
		PriGetExistsTime             string `json:"pri.get.exists_time"`                 // time spent in successful gets on primaries
		GetExistsTotal               int    `json:"get.exists_total,string"`             // number of successful gets on primaries & replicas
		PriGetExistsTotal            int    `json:"pri.get.exists_total,string"`         // number of successful gets on primaries
		GetMissingTime               string `json:"get.missing_time"`                    // time spent in failed gets on primaries & replicas
		PriGetMissingTime            string `json:"pri.get.missing_time"`                // time spent in failed gets on primaries
		GetMissingTotal              int    `json:"get.missing_total,string"`            // number of failed gets on primaries & replicas
		PriGetMissingTotal           int    `json:"pri.get.missing_total,string"`        // number of failed gets on primaries
		IndexingDeleteCurrent        int    `json:"indexing.delete_current,string"`      // number of current deletions on primaries & replicas
		PriIndexingDeleteCurrent     int    `json:"pri.indexing.delete_current,string"`  // number of current deletions on primaries
		IndexingDeleteTime           string `json:"indexing.delete_time"`                // time spent in deletions on primaries & replicas
		PriIndexingDeleteTime        string `json:"pri.indexing.delete_time"`            // time spent in deletions on primaries
		IndexingDeleteTotal          int    `json:"indexing.delete_total,string"`        // number of delete ops on primaries & replicas
		PriIndexingDeleteTotal       int    `json:"pri.indexing.delete_total,string"`    // number of delete ops on primaries
		IndexingIndexCurrent         int    `json:"indexing.index_current,string"`       // number of current indexing on primaries & replicas
		PriIndexingIndexCurrent      int    `json:"pri.indexing.index_current,string"`   // number of current indexing on primaries
		IndexingIndexTime            string `json:"indexing.index_time"`                 // time spent in indexing on primaries & replicas
		PriIndexingIndexTime         string `json:"pri.indexing.index_time"`             // time spent in indexing on primaries
		IndexingIndexTotal           int    `json:"indexing.index_total,string"`         // number of index ops on primaries & replicas
		PriIndexingIndexTotal        int    `json:"pri.indexing.index_total,string"`     // number of index ops on primaries
		IndexingIndexFailed          int    `json:"indexing.index_failed,string"`        // number of failed indexing ops on primaries & replicas
		PriIndexingIndexFailed       int    `json:"pri.indexing.index_failed,string"`    // number of failed indexing ops on primaries
		MergesCurrent                int    `json:"merges.current,string"`               // number of current merges on primaries & replicas
		PriMergesCurrent             int    `json:"pri.merges.current,string"`           // number of current merges on primaries
		MergesCurrentDocs            int    `json:"merges.current_docs,string"`          // number of current merging docs on primaries & replicas
		PriMergesCurrentDocs         int    `json:"pri.merges.current_docs,string"`      // number of current merging docs on primaries
		MergesCurrentSize            string `json:"merges.current_size"`                 // size of current merges on primaries & replicas
		PriMergesCurrentSize         string `json:"pri.merges.current_size"`             // size of current merges on primaries
		MergesTotal                  int    `json:"merges.total,string"`                 // number of completed merge ops on primaries & replicas
		PriMergesTotal               int    `json:"pri.merges.total,string"`             // number of completed merge ops on primaries
		MergesTotalDocs              int    `json:"merges.total_docs,string"`            // docs merged on primaries & replicas
		PriMergesTotalDocs           int    `json:"pri.merges.total_docs,string"`        // docs merged on primaries
		MergesTotalSize              string `json:"merges.total_size"`                   // size merged on primaries & replicas
		PriMergesTotalSize           string `json:"pri.merges.total_size"`               // size merged on primaries
		MergesTotalTime              string `json:"merges.total_time"`                   // time spent in merges on primaries & replicas
		PriMergesTotalTime           string `json:"pri.merges.total_time"`               // time spent in merges on primaries
		RefreshTotal                 int    `json:"refresh.total,string"`                // total refreshes on primaries & replicas
		PriRefreshTotal              int    `json:"pri.refresh.total,string"`            // total refreshes on primaries
		RefreshExternalTotal         int    `json:"refresh.external_total,string"`       // total external refreshes on primaries & replicas
		PriRefreshExternalTotal      int    `json:"pri.refresh.external_total,string"`   // total external refreshes on primaries
		RefreshTime                  string `json:"refresh.time"`                        // time spent in refreshes on primaries & replicas
		PriRefreshTime               string `json:"pri.refresh.time"`                    // time spent in refreshes on primaries
		RefreshExternalTime          string `json:"refresh.external_time"`               // external time spent in refreshes on primaries & replicas
		PriRefreshExternalTime       string `json:"pri.refresh.external_time"`           // external time spent in refreshes on primaries
		RefreshListeners             int    `json:"refresh.listeners,string"`            // number of pending refresh listeners on primaries & replicas
		PriRefreshListeners          int    `json:"pri.refresh.listeners,string"`        // number of pending refresh listeners on primaries
		SearchFetchCurrent           int    `json:"search.fetch_current,string"`         // current fetch phase ops on primaries & replicas
		PriSearchFetchCurrent        int    `json:"pri.search.fetch_current,string"`     // current fetch phase ops on primaries
		SearchFetchTime              string `json:"search.fetch_time"`                   // time spent in fetch phase on primaries & replicas
		PriSearchFetchTime           string `json:"pri.search.fetch_time"`               // time spent in fetch phase on primaries
		SearchFetchTotal             int    `json:"search.fetch_total,string"`           // total fetch ops on primaries & replicas
		PriSearchFetchTotal          int    `json:"pri.search.fetch_total,string"`       // total fetch ops on primaries
		SearchOpenContexts           int    `json:"search.open_contexts,string"`         // open search contexts on primaries & replicas
		PriSearchOpenContexts        int    `json:"pri.search.open_contexts,string"`     // open search contexts on primaries
		SearchQueryCurrent           int    `json:"search.query_current,string"`         // current query phase ops on primaries & replicas
		PriSearchQueryCurrent        int    `json:"pri.search.query_current,string"`     // current query phase ops on primaries
		SearchQueryTime              string `json:"search.query_time"`                   // time spent in query phase on primaries & replicas, e.g. "0s"
		PriSearchQueryTime           string `json:"pri.search.query_time"`               // time spent in query phase on primaries, e.g. "0s"
		SearchQueryTotal             int    `json:"search.query_total,string"`           // total query phase ops on primaries & replicas
		PriSearchQueryTotal          int    `json:"pri.search.query_total,string"`       // total query phase ops on primaries
		SearchScrollCurrent          int    `json:"search.scroll_current,string"`        // open scroll contexts on primaries & replicas
		PriSearchScrollCurrent       int    `json:"pri.search.scroll_current,string"`    // open scroll contexts on primaries
		SearchScrollTime             string `json:"search.scroll_time"`                  // time scroll contexts held open on primaries & replicas, e.g. "0s"
		PriSearchScrollTime          string `json:"pri.search.scroll_time"`              // time scroll contexts held open on primaries, e.g. "0s"
		SearchScrollTotal            int    `json:"search.scroll_total,string"`          // completed scroll contexts on primaries & replicas
		PriSearchScrollTotal         int    `json:"pri.search.scroll_total,string"`      // completed scroll contexts on primaries
		SearchThrottled              bool   `json:"search.throttled,string"`             // indicates if the index is search throttled
		SegmentsCount                int    `json:"segments.count,string"`               // number of segments on primaries & replicas
		PriSegmentsCount             int    `json:"pri.segments.count,string"`           // number of segments on primaries
		SegmentsMemory               string `json:"segments.memory"`                     // memory used by segments on primaries & replicas, e.g. "1.3kb"
		PriSegmentsMemory            string `json:"pri.segments.memory"`                 // memory used by segments on primaries, e.g. "1.3kb"
		SegmentsIndexWriterMemory    string `json:"segments.index_writer_memory"`        // memory used by index writer on primaries & replicas, e.g. "0b"
		PriSegmentsIndexWriterMemory string `json:"pri.segments.index_writer_memory"`    // memory used by index writer on primaries, e.g. "0b"
		SegmentsVersionMapMemory     string `json:"segments.version_map_memory"`         // memory used by version map on primaries & replicas, e.g. "0b"
		PriSegmentsVersionMapMemory  string `json:"pri.segments.version_map_memory"`     // memory used by version map on primaries, e.g. "0b"
		SegmentsFixedBitsetMemory    string `json:"segments.fixed_bitset_memory"`        // memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields on primaries & replicas, e.g. "0b"
		PriSegmentsFixedBitsetMemory string `json:"pri.segments.fixed_bitset_memory"`    // memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields on primaries, e.g. "0b"
		WarmerCurrent                int    `json:"warmer.current,string"`               // current warmer ops on primaries & replicas
		PriWarmerCurrent             int    `json:"pri.warmer.current,string"`           // current warmer ops on primaries
		WarmerTotal                  int    `json:"warmer.total,string"`                 // total warmer ops on primaries & replicas
		PriWarmerTotal               int    `json:"pri.warmer.total,string"`             // total warmer ops on primaries
		WarmerTotalTime              string `json:"warmer.total_time"`                   // time spent in warmers on primaries & replicas, e.g. "47s"
		PriWarmerTotalTime           string `json:"pri.warmer.total_time"`               // time spent in warmers on primaries, e.g. "47s"
		SuggestCurrent               int    `json:"suggest.current,string"`              // number of current suggest ops on primaries & replicas
		PriSuggestCurrent            int    `json:"pri.suggest.current,string"`          // number of current suggest ops on primaries
		SuggestTime                  string `json:"suggest.time"`                        // time spend in suggest on primaries & replicas, "31s"
		PriSuggestTime               string `json:"pri.suggest.time"`                    // time spend in suggest on primaries, e.g. "31s"
		SuggestTotal                 int    `json:"suggest.total,string"`                // number of suggest ops on primaries & replicas
		PriSuggestTotal              int    `json:"pri.suggest.total,string"`            // number of suggest ops on primaries
		MemoryTotal                  string `json:"memory.total"`                        // total user memory on primaries & replicas, e.g. "1.5kb"
		PriMemoryTotal               string `json:"pri.memory.total"`                    // total user memory on primaries, e.g. "1.5kb"
	}

	GetResult struct {
		Index       string                 `json:"_index"`   // index meta field
		Type        string                 `json:"_type"`    // type meta field
		Id          string                 `json:"_id"`      // id meta field
		Uid         string                 `json:"_uid"`     // uid meta field (see MapperService.java for all meta fields)
		Routing     string                 `json:"_routing"` // routing meta field
		Parent      string                 `json:"_parent"`  // parent meta field
		Version     *int64                 `json:"_version"` // version number, when Version is set to true in SearchService
		SeqNo       *int64                 `json:"_seq_no"`
		PrimaryTerm *int64                 `json:"_primary_term"`
		Source      json.RawMessage        `json:"_source,omitempty"`
		Found       bool                   `json:"found,omitempty"`
		Fields      map[string]interface{} `json:"fields,omitempty"`
		//Error     string                 `json:"error,omitempty"` // used only in MultiGet
		// TODO double-check that MultiGet now returns details error information
		Error *ErrorDetails `json:"error,omitempty"` // only used in MultiGet
	}

	ErrorDetails struct {
		Type         string                   `json:"type"`
		Reason       string                   `json:"reason"`
		ResourceType string                   `json:"resource.type,omitempty"`
		ResourceId   string                   `json:"resource.id,omitempty"`
		Index        string                   `json:"index,omitempty"`
		Phase        string                   `json:"phase,omitempty"`
		Grouped      bool                     `json:"grouped,omitempty"`
		CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
		RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
		Suppressed   []*ErrorDetails          `json:"suppressed,omitempty"`
		FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
		Header       map[string]interface{}   `json:"header,omitempty"`

		// ScriptException adds the information in the following block.

		ScriptStack []string             `json:"script_stack,omitempty"` // from ScriptException
		Script      string               `json:"script,omitempty"`       // from ScriptException
		Lang        string               `json:"lang,omitempty"`         // from ScriptException
		Position    *ScriptErrorPosition `json:"position,omitempty"`     // from ScriptException (7.7+)
	}

	ScriptErrorPosition struct {
		Offset int `json:"offset"`
		Start  int `json:"start"`
		End    int `json:"end"`
	}

	SearchResult struct {
		Header          http.Header          `json:"-"`
		TookInMillis    int64                `json:"took,omitempty"`             // search time in milliseconds
		TerminatedEarly bool                 `json:"terminated_early,omitempty"` // request terminated early
		NumReducePhases int                  `json:"num_reduce_phases,omitempty"`
		Clusters        *SearchResultCluster `json:"_clusters,omitempty"`  // 6.1.0+
		ScrollId        string               `json:"_scroll_id,omitempty"` // only used with Scroll and Scan operations
		Hits            *SearchHits          `json:"hits,omitempty"`       // the actual search hits
		// Suggest         SearchSuggest        `json:"suggest,omitempty"`      // results from suggesters
		// Aggregations    Aggregations         `json:"aggregations,omitempty"` // results from aggregations
		TimedOut bool          `json:"timed_out,omitempty"` // true if the search timed out
		Error    *ErrorDetails `json:"error,omitempty"`     // only used in MultiGet
		// Profile         *SearchProfile       `json:"profile,omitempty"`      // profiling results, if optional Profile API was active for this search
		// Shards          *ShardsInfo          `json:"_shards,omitempty"`      // shard information
		Status int    `json:"status,omitempty"` // used in MultiSearch
		PitId  string `json:"pit_id,omitempty"` // Point In Time ID
	}

	// SearchResultCluster holds information about a search response
	// from a cluster.
	SearchResultCluster struct {
		Successful int `json:"successful,omitempty"`
		Total      int `json:"total,omitempty"`
		Skipped    int `json:"skipped,omitempty"`
	}
	SearchHits struct {
		TotalHits *TotalHits   `json:"total,omitempty"`     // total number of hits found
		MaxScore  *float64     `json:"max_score,omitempty"` // maximum score of all hits
		Hits      []*SearchHit `json:"hits,omitempty"`      // the actual hits returned
	}
	TotalHits struct {
		Value    int64  `json:"value"`    // value of the total hit count
		Relation string `json:"relation"` // how the value should be interpreted: accurate ("eq") or a lower bound ("gte")
	}

	SearchHit struct {
		Score       *float64      `json:"_score,omitempty"`   // computed score
		Index       string        `json:"_index,omitempty"`   // index name
		Type        string        `json:"_type,omitempty"`    // type meta field
		Id          string        `json:"_id,omitempty"`      // external or internal
		Uid         string        `json:"_uid,omitempty"`     // uid meta field (see MapperService.java for all meta fields)
		Routing     string        `json:"_routing,omitempty"` // routing meta field
		Parent      string        `json:"_parent,omitempty"`  // parent meta field
		Version     *int64        `json:"_version,omitempty"` // version number, when Version is set to true in SearchService
		SeqNo       *int64        `json:"_seq_no"`
		PrimaryTerm *int64        `json:"_primary_term"`
		Sort        []interface{} `json:"sort,omitempty"` // sort information
		// Highlight      SearchHitHighlight             `json:"highlight,omitempty"`       // highlighter information
		Source json.RawMessage `json:"_source,omitempty"` // stored document source
		// Fields         SearchHitFields                `json:"fields,omitempty"`          // returned (stored) fields
		// Explanation    *SearchExplanation             `json:"_explanation,omitempty"`    // explains how the score was computed
		MatchedQueries []string `json:"matched_queries,omitempty"` // matched queries
		// InnerHits      map[string]*SearchHitInnerHits `json:"inner_hits,omitempty"`      // inner hits with ES >= 1.5.0
		// Nested         *NestedHit                     `json:"_nested,omitempty"`         // for nested inner hits
		Shard string `json:"_shard,omitempty"` // used e.g. in Search Explain
		Node  string `json:"_node,omitempty"`  // used e.g. in Search Explain

		// HighlightFields
		// SortValues
		// MatchedFilters
	}
)
