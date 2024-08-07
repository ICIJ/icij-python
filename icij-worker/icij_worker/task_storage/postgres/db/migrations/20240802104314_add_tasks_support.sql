-- migrate:up
CREATE TABLE tasks (
    id varchar(128),
    name varchar(128) NOT NULL,
    namespace varchar(128),
    state varchar(16) NOT NULL,
    progress real,
    created_at timestamptz NOT NULL,
    completed_at timestamptz,
    cancelled_at timestamptz,
    retries_left smallint NOT NULL,
    max_retries smallint, -- TODO: might not be supported in Java yet, switch to NOT NULL later
    arguments varchar,
    PRIMARY KEY(id)
);
CREATE INDEX index_tasks_created_at ON tasks (created_at);
CREATE INDEX index_tasks_state ON tasks (state);

/*
TODO: discuss the varchar vs jsonb here, since we don't need validation + compression varchar might be OK
*/
CREATE TABLE results (
    task_id varchar(128) references tasks(id),
    result varchar,
    created_at timestamptz NOT NULL,
    PRIMARY KEY(task_id)
);
CREATE INDEX index_results_created_at ON results (created_at);

/*
TODO: decide on the actual size of field here
*/
CREATE TABLE errors (
    task_id varchar(128) NOT NULL references tasks(id),
    name varchar(128) NOT NULL,
    message varchar NOT NULL,
    cause varchar,
    stacktrace varchar, -- we store trace as json, which is kind of dirty but JSON is fairly readable
    retries_left smallint NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE INDEX index_errors_created_at ON errors (created_at);

-- migrate:down
DROP table tasks;
DROP index index_tasks_created_at;
DROP table results;
DROP index index_results_created_at;
DROP table errors;
DROP index index_errors_created_at;
