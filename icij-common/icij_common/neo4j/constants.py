# Should be DB, but week keep it like this to avoid migrating the registry
DATABASE_REGISTRY_DB = "datashare-project-registry"

DATABASE_RUNS_MIGRATION = "_RUNS"
DATABASE_NAME = "name"
# Should be DB, but week keep it like this to avoid migrating the registry
DATABASE_NODE = "_Project"

MIGRATION_COMPLETED = "completed"
MIGRATION_LABEL = "label"
MIGRATION_NODE = "_Migration"
# Should be DB, but week keep it like this to avoid migrating the registry
MIGRATION_DB = "project"
MIGRATION_STARTED = "started"
MIGRATION_STATUS = "status"
MIGRATION_VERSION = "version"

TASK_NODE = "_Task"
TASK_ARGUMENTS = "arguments"
TASK_CANCELLED_AT = "cancelledAt"
TASK_COMPLETED_AT = "completedAt"
TASK_CREATED_AT = "createdAt"
TASK_ID = "id"
TASK_INPUTS_DEPRECATED = "inputs"
TASK_NAME = "name"
TASK_NAMESPACE = "namespace"
TASK_PROGRESS = "progress"
TASK_RETRIES = "retries"
TASK_TYPE_DEPRECATED = "type"

TASK_CANCEL_EVENT_NODE = "_CancelEvent"
TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED = "createdAt"
TASK_CANCEL_EVENT_CANCELLED_AT = "cancelledAt"
TASK_CANCEL_EVENT_EFFECTIVE = "effective"
TASK_CANCEL_EVENT_REQUEUE = "requeue"
TASK_CANCELLED_BY_EVENT_REL = "_CANCELLED_BY"

TASK_LOCK_NODE = "_TaskLock"
TASK_LOCK_TASK_ID = "taskId"
TASK_LOCK_WORKER_ID = "workerId"

TASK_ERROR_NODE = "_TaskError"
TASK_ERROR_DETAIL_DEPRECATED = "detail"  # use stacktrace
TASK_ERROR_ID = "id"
TASK_ERROR_MESSAGE = "message"
TASK_ERROR_NAME = "name"
TASK_ERROR_OCCURRED_AT = "occurredAt"
TASK_ERROR_STACKTRACE = "stacktrace"
TASK_ERROR_TITLE_DEPRECATED = "title"  # use message

TASK_ERROR_OCCURRED_TYPE = "_OCCURRED_DURING"

TASK_RESULT_NODE = "_TaskResult"
TASK_HAS_RESULT_TYPE = "_HAS_RESULT"
TASK_RESULT_RESULT = "result"
