package history

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	taskTimeout          = time.Second * 10 * debug.TimeoutMultiplier
	taskHistoryOpTimeout = 20 * time.Second
)

var (
	errUnknownTransferTask = serviceerror.NewInternal("Unknown transfer task")
)

type (
	transferQueueTaskExecutorBase struct {
		currentClusterName       string
		shardContext             historyi.ShardContext
		registry                 namespace.Registry
		cache                    wcache.Cache
		logger                   log.Logger
		metricHandler            metrics.Handler
		historyRawClient         resource.HistoryRawClient
		matchingRawClient        resource.MatchingRawClient
		config                   *configs.Config
		searchAttributesProvider searchattribute.Provider
		visibilityManager        manager.VisibilityManager
		workflowDeleteManager    deletemanager.DeleteManager
		chasmEngine              chasm.Engine
	}
)

func newTransferQueueTaskExecutorBase(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricHandler metrics.Handler,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
	chasmEngine chasm.Engine,
) *transferQueueTaskExecutorBase {
	return &transferQueueTaskExecutorBase{
		currentClusterName:       shardContext.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:             shardContext,
		registry:                 shardContext.GetNamespaceRegistry(),
		cache:                    workflowCache,
		logger:                   logger,
		metricHandler:            metricHandler,
		historyRawClient:         historyRawClient,
		matchingRawClient:        matchingRawClient,
		config:                   shardContext.GetConfig(),
		searchAttributesProvider: shardContext.GetSearchAttributesProvider(),
		visibilityManager:        visibilityManager,
		workflowDeleteManager: deletemanager.NewDeleteManager(
			shardContext,
			workflowCache,
			shardContext.GetConfig(),
			shardContext.GetTimeSource(),
			visibilityManager,
		),
		chasmEngine: chasmEngine,
	}
}

func (t *transferQueueTaskExecutorBase) pushActivity(
	ctx context.Context,
	task *tasks.ActivityTask,
	activityScheduleToStartTimeout time.Duration,
	directive *taskqueuespb.TaskVersionDirective,
	priority *commonpb.Priority,
	transactionPolicy historyi.TransactionPolicy,
) error {
	resp, err := t.matchingRawClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: durationpb.New(activityScheduleToStartTimeout),
		Clock:                  vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), task.TaskID),
		VersionDirective:       directive,
		Stamp:                  task.Stamp,
		Priority:               priority,
	})
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		// NotFound error is not expected for AddTasks calls
		// but will be ignored by task error handling logic, so log it here
		tasks.InitializeLogger(task, t.logger).Error("Matching returned not found error for AddActivityTask", tag.Error(err))
	}

	if err != nil {
		return err
	}

	if directive.GetUseAssignmentRules() == nil {
		// activity is not getting a new build ID, so no need to update MS
		return nil
	}

	return updateIndependentActivityBuildId(
		ctx,
		task,
		resp.AssignedBuildId,
		t.shardContext,
		transactionPolicy,
		t.cache,
		t.metricHandler,
		t.logger,
	)
}

func (t *transferQueueTaskExecutorBase) pushWorkflowTask(
	ctx context.Context,
	task *tasks.WorkflowTask,
	taskqueue *taskqueuepb.TaskQueue,
	workflowTaskScheduleToStartTimeout time.Duration,
	directive *taskqueuespb.TaskVersionDirective,
	priority *commonpb.Priority,
	transactionPolicy historyi.TransactionPolicy,
) error {
	var sst *durationpb.Duration
	if workflowTaskScheduleToStartTimeout > 0 {
		sst = durationpb.New(workflowTaskScheduleToStartTimeout)
	}
	resp, err := t.matchingRawClient.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue:              taskqueue,
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: sst,
		Clock:                  vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), task.TaskID),
		VersionDirective:       directive,
		Priority:               priority,
	})
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		// NotFound error is not expected for AddTasks calls
		// but will be ignored by task error handling logic, so log it here
		tasks.InitializeLogger(task, t.logger).Error("Matching returned not found error for AddWorkflowTask", tag.Error(err))
	}

	if err != nil {
		return err
	}

	if directive.GetUseAssignmentRules() == nil {
		// assignment rules are not used, so no need to update MS
		return nil
	}

	return initializeWorkflowAssignedBuildId(
		ctx,
		task,
		resp.AssignedBuildId,
		t.shardContext,
		transactionPolicy,
		t.cache,
		t.metricHandler,
		t.logger,
	)
}

func (t *transferQueueTaskExecutorBase) processDeleteExecutionTask(
	ctx context.Context,
	task *tasks.DeleteExecutionTask,
	ensureNoPendingCloseTask bool,
) error {
	return t.deleteExecution(ctx, task, ensureNoPendingCloseTask, &task.ProcessStage)
}

func (t *transferQueueTaskExecutorBase) deleteExecution(
	ctx context.Context,
	task tasks.Task,
	ensureNoPendingCloseTask bool,
	stage *tasks.DeleteWorkflowExecutionStage,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}

	weCtx, release, err := t.cache.GetOrCreateChasmEntity(
		ctx,
		t.shardContext,
		namespace.ID(task.GetNamespaceID()),
		&workflowExecution,
		chasm.ArchetypeAny, // deletion logic works for all Archetypes.
		locks.PriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weCtx, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}

	// Here, we ensure that the workflow is closed successfully before deleting it. Otherwise, the mutable state
	// might be deleted before the close task is executed, and so the close task will be dropped. In passive cluster,
	// this check can be ignored.
	//
	// Additionally, this function itself could be called from within the close execution task, so we need to skip
	// the check in that case because the close execution task would be waiting for itself to finish forever. So, the
	// ensureNoPendingCloseTask flag is set iff we're running in the active cluster, and we aren't processing the
	// CloseExecutionTask from within this same goroutine.
	if ensureNoPendingCloseTask {
		// Unfortunately, queue states/ack levels are updated with delay ("history.transferProcessorUpdateAckInterval", default 30s),
		// therefore this could fail if the workflow was closed before the queue state/ack levels were updated,
		// so we return a retryable error.
		if t.isCloseExecutionTaskPending(mutableState, weCtx) {
			return consts.ErrDependencyTaskNotCompleted
		}
	}

	// If task version is EmptyVersion it means "don't check task version".
	// This can happen when task was created from explicit user API call.
	// Or the namespace is a local namespace which will not have version conflict.
	taskVersion := common.EmptyVersion
	if taskWithVersion, ok := task.(tasks.HasVersion); ok {
		taskVersion = taskWithVersion.GetVersion()
	}

	if taskVersion != common.EmptyVersion {
		closeVersion, err := mutableState.GetCloseVersion()
		if err != nil {
			return err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, taskVersion, task)
		if err != nil {
			return err
		}
	}

	return t.workflowDeleteManager.DeleteWorkflowExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		&workflowExecution,
		weCtx,
		mutableState,
		stage,
	)
}

func (t *transferQueueTaskExecutorBase) isCloseExecutionTaskPending(ms historyi.MutableState, weCtx historyi.WorkflowContext) bool {
	closeTransferTaskId := ms.GetExecutionInfo().CloseTransferTaskId
	// taskID == 0 if workflow closed before this field was added (v1.17).
	if closeTransferTaskId == 0 {
		return false
	}
	// check if close execution transfer task is completed
	transferQueueState, ok := t.shardContext.GetQueueState(tasks.CategoryTransfer)
	if !ok {
		return true
	}
	fakeCloseTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: weCtx.GetWorkflowKey(),
		TaskID:      closeTransferTaskId,
	}
	return !queues.IsTaskAcked(fakeCloseTransferTask, transferQueueState)
}
