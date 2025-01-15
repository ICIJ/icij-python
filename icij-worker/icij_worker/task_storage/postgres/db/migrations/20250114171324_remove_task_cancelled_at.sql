-- migrate:up
UPDATE tasks
SET completed_at = cancelled_at
WHERE state = 'CANCELLED';

ALTER TABLE tasks
DROP COLUMN cancelled_at;

-- migrate:down
ALTER TABLE tasks
ADD cancelled_at timestamptz;

UPDATE tasks
SET cancelled_at = completed_at
WHERE state = 'CANCELLED';
