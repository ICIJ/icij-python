-- migrate:up
CREATE TABLE task_parents (
    task_id varchar(128)  NOT NULL references tasks(id),
    parent_id varchar(128)  NOT NULL references tasks(id),
    provided_argument varchar(128) NOT NULL
);

-- migrate:down
DROP table task_parents;
