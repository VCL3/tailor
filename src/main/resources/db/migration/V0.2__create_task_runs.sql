CREATE TABLE task_runs (
	id SERIAL PRIMARY KEY,
  task_rule_id INTEGER REFERENCES task_rules(id),
  status VARCHAR(30),
  state_snapshot TEXT,
  count INTEGER,
  published_count INTEGER,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)