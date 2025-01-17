-- migrate:up
ALTER TABLE tasks RENAME COLUMN "group" TO "group_id";

-- migrate:down
ALTER TABLE tasks RENAME COLUMN "group_id" TO "group_id";
