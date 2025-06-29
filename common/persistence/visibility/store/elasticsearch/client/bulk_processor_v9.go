package client

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v9/esutil"
)

type (
	bulkIndexerImpl struct {
		ctx context.Context
		es  esutil.BulkIndexer
	}
)

func newBulkProcessor_n(ctx context.Context, cfg esutil.BulkIndexerConfig) (*bulkIndexerImpl, error) {
	indexer, err := esutil.NewBulkIndexer(cfg)
	if err != nil {
		return nil, err
	}
	return &bulkIndexerImpl{
		ctx: ctx,
		es:  indexer,
	}, nil
}

func (b *bulkIndexerImpl) Add(request *BulkIndexerRequest) error {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkIndexRequest := esutil.BulkIndexerItem{
			Index:       request.Index,
			Action:      "index",
			DocumentID:  request.ID,
			Version:     request.Version,
			VersionType: versionTypeExternal,
			Body:        request.Doc,
		}
		return b.es.Add(b.ctx, bulkIndexRequest)

	case BulkableRequestTypeDelete:
		bulkDeleteRequest := esutil.BulkIndexerItem{
			Index:       request.Index,
			Action:      "delete",
			DocumentID:  request.ID,
			Version:     request.Version,
			VersionType: versionTypeExternal,
		}
		return b.es.Add(b.ctx, bulkDeleteRequest)
	default:
		return fmt.Errorf("unsupported request type: %v", request.RequestType)
	}
}

func (b *bulkIndexerImpl) Stop() error {
	return b.es.Close(b.ctx)
}
