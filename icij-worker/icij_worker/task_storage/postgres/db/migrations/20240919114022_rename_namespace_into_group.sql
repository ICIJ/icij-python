-- migrate:up
ALTER TABLE tasks RENAME COLUMN "namespace" TO "group";

-- migrate:down
ALTER TABLE tasks RENAME COLUMN "group" TO "namespace";
