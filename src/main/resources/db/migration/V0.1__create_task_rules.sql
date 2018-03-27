CREATE TABLE task_rules (
  id SERIAL PRIMARY KEY,
  status VARCHAR(100) NOT NULL,
  search_params text,
  recurrence text,
  source VARCHAR(30),
  max_records INTEGER,
  type VARCHAR(30),
  urls_fetched INTEGER,
  country VARCHAR(30),
  auto_start BOOLEAN,
  task_bucket VARCHAR(30),
  client_id VARCHAR(30) NOT NULL,
  next_schedule_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX rule_schedule_idx ON task_rules (status, next_schedule_at);
CREATE INDEX source_idx ON task_rules (source);
CREATE INDEX status_idx ON task_rules (status);