-- migrate:up
ALTER TABLE tasks RENAME COLUMN arguments TO args;

-- migrate:down
ALTER TABLE tasks RENAME COLUMN args TO arguments;
