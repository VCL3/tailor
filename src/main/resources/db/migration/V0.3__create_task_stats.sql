CREATE TABLE task_stats (
	id SERIAL PRIMARY KEY,
	task_rule_id INTEGER REFERENCES task_rules(id),
  task_run_id INTEGER REFERENCES task_runs(id),
  stats_snapshot TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP
)