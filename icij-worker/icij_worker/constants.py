# AMQP
from icij_common.pydantic_utils import to_lower_camel


AMQP_HEALTH_X = "exchangeMonitoring"
AMQP_HEALTH_ROUTING_KEY = "routingKeyMonitoring"
AMQP_HEALTH_QUEUE = "MONITORING"

AMQP_MANAGER_EVENTS_X = "exchangeManagerEvents"
AMQP_MANAGER_EVENTS_QUEUE = "MANAGER_EVENT"
AMQP_MANAGER_EVENTS_ROUTING_KEY = "routingKeyManagerEvents"

AMQP_MANAGER_EVENTS_DL_X = "exchangeDLQManagerEvents"
AMQP_MANAGER_EVENTS_DL_QUEUE = "MANAGER_EVENT_DLQ"
AMQP_MANAGER_EVENTS_DL_ROUTING_KEY = "routingKeyDLQManagerEvents"

AMQP_TASKS_DL_X = "exchangeDLQTasks"
AMQP_TASKS_DL_QUEUE = "TASK_DLQ"
AMQP_TASKS_DL_ROUTING_KEY = "routingKeyDLQTasks"

AMQP_TASKS_X = "exchangeMainTasks"
AMQP_TASKS_QUEUE = "TASK"
AMQP_TASKS_ROUTING_KEY = "routingKeyMainTasks"

AMQP_WORKER_EVENTS_X = "exchangeWorkerEvents"
AMQP_WORKER_EVENTS_QUEUE = "WORKER_EVENT"
AMQP_WORKER_EVENTS_ROUTING_KEY = "routingKeyWorkerEvents"

AMQP_TASK_QUEUE_PRIORITY = 1000
AMQP_HEALTH_POLICY_PRIORITY = 10000

_CREATED_AT = "created_at"
_TASK_ID = "task_id"

# General
TASK_ARGUMENTS_DEPRECATED = "arguments"
TASK_ARGS = "args"
TASK_CANCELLED_AT_DEPRECATED = "cancelled_at"
TASK_COMPLETED_AT = "completed_at"
TASK_CREATED_AT = _CREATED_AT
TASK_GROUP = "group"
TASK_ID = "id"
TASK_MAX_RETRIES = "max_retries"
TASK_NAME = "name"
TASK_NAMESPACE_DEPRECATED = "namespace"
TASK_PROGRESS = "progress"
TASK_RETRIES_LEFT = "retries_left"
TASK_STATE = "state"

TASK_ERRORS_TASK_ID = _TASK_ID

TASK_RESULT_CREATED_AT = _CREATED_AT
TASK_RESULT_TASK_ID = _TASK_ID
TASK_RESULT_RESULT = "result"
TASK_RESULT_RESULT_VALUE = "value"

# POSTGRES
POSTGRES_TASKS_GROUP = "group_id"

POSTGRES_TASKS_TABLE = "tasks"
POSTGRES_TASK_ERRORS_TABLE = "errors"
POSTGRES_TASK_RESULTS_TABLE = "results"

POSTGRES_TASK_DB_NAME = "name"
POSTGRES_TASK_DB_IS_LOCKED = "is_locked"
POSTGRES_TASK_DBS_TABLE = "task_dbs"

# Neo4j
NEO4J_TASK_NODE = "_Task"
NEO4J_TASK_ARGS = to_lower_camel(TASK_ARGS)
NEO4J_TASK_ARGUMENTS_DEPRECATED = to_lower_camel(TASK_ARGUMENTS_DEPRECATED)
NEO4J_TASK_CANCELLED_AT_DEPRECATED = to_lower_camel(TASK_CANCELLED_AT_DEPRECATED)
NEO4J_TASK_COMPLETED_AT = to_lower_camel(TASK_COMPLETED_AT)
NEO4J_TASK_CREATED_AT = to_lower_camel(TASK_CREATED_AT)
NEO4J_TASK_ID = to_lower_camel(TASK_ID)
NEO4J_TASK_INPUTS_DEPRECATED = "inputs"
NEO4J_TASK_GROUP = to_lower_camel(TASK_GROUP)
NEO4J_TASK_MAX_RETRIES = to_lower_camel(TASK_MAX_RETRIES)
NEO4J_TASK_NAME = to_lower_camel(TASK_NAME)
NEO4J_TASK_NAMESPACE_DEPRECATED = to_lower_camel(TASK_NAMESPACE_DEPRECATED)
NEO4J_TASK_PROGRESS = to_lower_camel(TASK_PROGRESS)
NEO4J_TASK_RETRIES_DEPRECATED = "retries"
NEO4J_TASK_RETRIES_LEFT = to_lower_camel(TASK_RETRIES_LEFT)
NEO4J_TASK_TYPE_DEPRECATED = "type"

NEO4J_TASK_CANCEL_EVENT_NODE = "_CancelEvent"
NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED = "createdAt"
NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT = "cancelledAt"
NEO4J_TASK_CANCEL_EVENT_EFFECTIVE = "effective"
NEO4J_TASK_CANCEL_EVENT_REQUEUE = "requeue"
NEO4J_TASK_CANCELLED_BY_EVENT_REL = "_CANCELLED_BY"

NEO4J_TASK_MANAGER_EVENT_NODE = "_ManagerEvent"
NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT = to_lower_camel(_CREATED_AT)
NEO4J_TASK_MANAGER_EVENT_EVENT = "event"

NEO4J_TASK_LOCK_NODE = "_TaskLock"
NEO4J_TASK_LOCK_TASK_ID = to_lower_camel(_TASK_ID)
NEO4J_TASK_LOCK_WORKER_ID = "workerId"

NEO4J_TASK_ERROR_NODE = "_TaskError"
NEO4J_TASK_ERROR_DETAIL_DEPRECATED = "detail"  # use stacktrace
NEO4J_TASK_ERROR_ID_DEPRECATED = "id"
NEO4J_TASK_ERROR_MESSAGE = "message"
NEO4J_TASK_ERROR_NAME = "name"
NEO4J_TASK_ERROR_OCCURRED_AT_DEPRECATED = "occurredAt"
NEO4J_TASK_ERROR_STACKTRACE = "stacktrace"
NEO4J_TASK_ERROR_TITLE_DEPRECATED = "title"  # use message

NEO4J_TASK_ERROR_OCCURRED_TYPE = "_OCCURRED_DURING"
NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT = "occurredAt"
NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT = "retriesLeft"

NEO4J_TASK_RESULT_NODE = "_TaskResult"
NEO4J_TASK_HAS_RESULT_TYPE = "_HAS_RESULT"
NEO4J_TASK_RESULT_RESULT = "result"

NEO4J_SHUTDOWN_EVENT_NODE = "_ShutdownEvent"
NEO4J_SHUTDOWN_EVENT_CREATED_AT = to_lower_camel(_CREATED_AT)

NEO4J_WORKER_NODE = "_Worker"
NEO4J_WORKER_ID = "id"
NEO4J_WORKER_SHUTDOWN_BY_REL = "_SHUTDOWN_BY"
