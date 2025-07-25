package client

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
)

var (
	retryPolicy               = common.CreatePersistenceClientRetryPolicy()
	namespaceQueueRetryPolicy = backoff.NewConstantDelayRetryPolicy(time.Millisecond * 50).WithMaximumAttempts(10)
)

type (
	// Factory defines the interface for any implementation that can vend
	// persistence layer objects backed by a datastore. The actual datastore
	// is implementation detail hidden behind this interface
	Factory interface {
		// Close the factory
		Close()
		// NewTaskManager returns a new task manager
		NewTaskManager() (persistence.TaskManager, error)
		// NewFairTaskManager returns a new fair task manager
		NewFairTaskManager() (persistence.FairTaskManager, error)
		// NewShardManager returns a new shard manager
		NewShardManager() (persistence.ShardManager, error)
		// NewMetadataManager returns a new metadata manager
		NewMetadataManager() (persistence.MetadataManager, error)
		// NewExecutionManager returns a new execution manager
		NewExecutionManager() (persistence.ExecutionManager, error)
		// NewNamespaceReplicationQueue returns a new queue for namespace replication
		NewNamespaceReplicationQueue() (persistence.NamespaceReplicationQueue, error)
		// NewClusterMetadataManager returns a new manager for cluster specific metadata
		NewClusterMetadataManager() (persistence.ClusterMetadataManager, error)
		// NewHistoryTaskQueueManager returns a new manager for history task queues
		NewHistoryTaskQueueManager() (persistence.HistoryTaskQueueManager, error)
		// NewNexusEndpointManager returns a new manager for nexus endpoints
		NewNexusEndpointManager() (persistence.NexusEndpointManager, error)
	}

	factoryImpl struct {
		dataStoreFactory     persistence.DataStoreFactory
		config               *config.Persistence
		serializer           serialization.Serializer
		eventBlobCache       persistence.XDCCache
		metricsHandler       metrics.Handler
		logger               log.Logger
		clusterName          string
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		shardRateLimiter     quotas.RequestRateLimiter
		healthSignals        persistence.HealthSignalAggregator
	}
)

// NewFactory returns an implementation of factory that vends persistence objects based on
// specified configuration. This factory takes as input a config.Persistence object
// which specifies the datastore to be used for a given type of object. This config
// also contains config for individual datastores themselves.
//
// The objects returned by this factory enforce ratelimit and maxconns according to
// given configuration. In addition, all objects will emit metrics automatically
func NewFactory(
	dataStoreFactory persistence.DataStoreFactory,
	cfg *config.Persistence,
	systemRateLimiter quotas.RequestRateLimiter,
	namespaceRateLimiter quotas.RequestRateLimiter,
	shardRateLimiter quotas.RequestRateLimiter,
	serializer serialization.Serializer,
	eventBlobCache persistence.XDCCache,
	clusterName string,
	metricsHandler metrics.Handler,
	logger log.Logger,
	healthSignals persistence.HealthSignalAggregator,
) Factory {
	factory := &factoryImpl{
		dataStoreFactory:     dataStoreFactory,
		config:               cfg,
		serializer:           serializer,
		eventBlobCache:       eventBlobCache,
		metricsHandler:       metricsHandler,
		logger:               logger,
		clusterName:          clusterName,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		shardRateLimiter:     shardRateLimiter,
		healthSignals:        healthSignals,
	}
	factory.initDependencies()
	return factory
}

// NewTaskManager returns a new task manager
func (f *factoryImpl) NewTaskManager() (persistence.TaskManager, error) {
	taskStore, err := f.dataStoreFactory.NewTaskStore()
	if err != nil {
		return nil, err
	}
	result := persistence.NewTaskManager(taskStore, f.serializer)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewTaskPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewTaskPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewTaskPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// NewFairTaskManager returns a new task fairness manager
func (f *factoryImpl) NewFairTaskManager() (persistence.FairTaskManager, error) {
	taskStore, err := f.dataStoreFactory.NewFairTaskStore()
	if err != nil {
		return nil, err
	}
	result := persistence.NewTaskManager(taskStore, f.serializer)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewTaskPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewTaskPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewTaskPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// NewShardManager returns a new shard manager
func (f *factoryImpl) NewShardManager() (persistence.ShardManager, error) {
	shardStore, err := f.dataStoreFactory.NewShardStore()
	if err != nil {
		return nil, err
	}

	result := persistence.NewShardManager(shardStore, f.serializer)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewShardPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewShardPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewShardPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// NewMetadataManager returns a new metadata manager
func (f *factoryImpl) NewMetadataManager() (persistence.MetadataManager, error) {
	store, err := f.dataStoreFactory.NewMetadataStore()
	if err != nil {
		return nil, err
	}

	result := persistence.NewMetadataManagerImpl(store, f.serializer, f.logger, f.clusterName)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewMetadataPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewMetadataPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewMetadataPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// NewClusterMetadataManager returns a new cluster metadata manager
func (f *factoryImpl) NewClusterMetadataManager() (persistence.ClusterMetadataManager, error) {
	store, err := f.dataStoreFactory.NewClusterMetadataStore()
	if err != nil {
		return nil, err
	}

	result := persistence.NewClusterMetadataManagerImpl(store, f.serializer, f.clusterName, f.logger)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewClusterMetadataPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewClusterMetadataPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewClusterMetadataPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// NewExecutionManager returns a new execution manager
func (f *factoryImpl) NewExecutionManager() (persistence.ExecutionManager, error) {
	store, err := f.dataStoreFactory.NewExecutionStore()
	if err != nil {
		return nil, err
	}

	result := persistence.NewExecutionManager(store, f.serializer, f.eventBlobCache, f.logger, f.config.TransactionSizeLimit)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewExecutionPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewExecutionPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewExecutionPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

func (f *factoryImpl) NewNamespaceReplicationQueue() (persistence.NamespaceReplicationQueue, error) {
	result, err := f.dataStoreFactory.NewQueue(persistence.NamespaceReplicationQueueType)
	if err != nil {
		return nil, err
	}

	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewQueuePersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewQueuePersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewQueuePersistenceRetryableClient(result, namespaceQueueRetryPolicy, IsNamespaceQueueTransientError)
	return persistence.NewNamespaceReplicationQueue(result, f.serializer, f.clusterName, f.metricsHandler, f.logger)
}

func (f *factoryImpl) NewHistoryTaskQueueManager() (persistence.HistoryTaskQueueManager, error) {
	q, err := f.dataStoreFactory.NewQueueV2()
	if err != nil {
		return nil, err
	}
	return persistence.NewHistoryTaskQueueManager(q, serialization.NewSerializer()), nil
}

func (f *factoryImpl) NewNexusEndpointManager() (persistence.NexusEndpointManager, error) {
	store, err := f.dataStoreFactory.NewNexusEndpointStore()
	if err != nil {
		return nil, err
	}

	result := persistence.NewNexusEndpointManager(store, f.serializer, f.logger)
	if f.systemRateLimiter != nil && f.namespaceRateLimiter != nil {
		result = persistence.NewNexusEndpointPersistenceRateLimitedClient(result, f.systemRateLimiter, f.namespaceRateLimiter, f.shardRateLimiter, f.logger)
	}
	if f.metricsHandler != nil && f.healthSignals != nil {
		result = persistence.NewNexusEndpointPersistenceMetricsClient(result, f.metricsHandler, f.healthSignals, f.logger)
	}
	result = persistence.NewNexusEndpointPersistenceRetryableClient(result, retryPolicy, IsPersistenceTransientError)
	return result, nil
}

// Close closes this factory
func (f *factoryImpl) Close() {
	f.dataStoreFactory.Close()
	if f.healthSignals != nil {
		f.healthSignals.Stop()
	}
}

func IsPersistenceTransientError(err error) bool {
	switch err.(type) {
	// we retry on DataLoss errors because persistence layer is sometimes unreliable when we immediately read-after-write
	case *serviceerror.Unavailable, *serviceerror.DataLoss:
		return true
	}

	return false
}

func IsNamespaceQueueTransientError(err error) bool {
	switch err.(type) {
	case *serviceerror.Unavailable, *persistence.ConditionFailedError:
		return true
	}

	return false
}

func (f *factoryImpl) initDependencies() {
	if f.metricsHandler == nil && f.healthSignals == nil {
		return
	}

	if f.metricsHandler == nil {
		f.metricsHandler = metrics.NoopMetricsHandler
	}
	if f.healthSignals == nil {
		f.healthSignals = persistence.NoopHealthSignalAggregator
	}
	f.healthSignals.Start()
}
