-- migrate:up
CREATE TABLE task_argument_providers (
    task_id varchar(128)  NOT NULL references tasks(id),
    provider_id varchar(128)  NOT NULL references tasks(id),
    provided_argument varchar(128) NOT NULL
);

-- migrate:down
DROP table task_argument_providers;
