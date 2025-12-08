-- Agent Registry Schema
CREATE TABLE IF NOT EXISTS agents (
    agent_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL CHECK(agent_type IN ('orchestrator', 'ingestion', 'processing', 'storage', 'monitoring')),
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'idle' CHECK(status IN ('idle', 'busy', 'degraded', 'error', 'offline')),
    capabilities TEXT, -- JSON array
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP,
    metadata TEXT -- JSON object
);

CREATE INDEX idx_agents_type ON agents(agent_type);
CREATE INDEX idx_agents_status ON agents(status);

-- Pipeline Executions
CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'paused', 'completed', 'failed')),
    config TEXT NOT NULL, -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Tasks
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'assigned', 'running', 'completed', 'failed', 'retrying')),
    priority INTEGER DEFAULT 5,
    task_data TEXT, -- JSON
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    assigned_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id),
    FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

CREATE INDEX idx_tasks_pipeline ON tasks(pipeline_id);
CREATE INDEX idx_tasks_agent ON tasks(agent_id);
CREATE INDEX idx_tasks_status ON tasks(status);

-- Data Source Metadata
CREATE TABLE IF NOT EXISTS data_sources (
    source_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_url TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'paused', 'completed', 'error')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata TEXT -- JSON
);

-- Frame/Data Registry
CREATE TABLE IF NOT EXISTS frame_metadata (
    frame_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    sequence_number INTEGER,
    timestamp TIMESTAMP NOT NULL,
    fps INTEGER,
    storage_path TEXT,
    file_hash TEXT,
    privacy_tag TEXT,
    processing_status TEXT CHECK(processing_status IN ('ingested', 'processed', 'stored', 'failed')),
    ingested_at TIMESTAMP,
    processed_at TIMESTAMP,
    stored_at TIMESTAMP,
    metadata TEXT, -- JSON
    FOREIGN KEY (source_id) REFERENCES data_sources(source_id),
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
);

CREATE INDEX idx_frames_source ON frame_metadata(source_id);
CREATE INDEX idx_frames_pipeline ON frame_metadata(pipeline_id);
CREATE INDEX idx_frames_timestamp ON frame_metadata(timestamp);

-- Metrics & Monitoring
CREATE TABLE IF NOT EXISTS agent_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cpu_usage REAL,
    memory_usage REAL,
    queue_length INTEGER,
    processing_rate REAL,
    error_count INTEGER DEFAULT 0,
    custom_metrics TEXT, -- JSON
    FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

CREATE INDEX idx_metrics_agent_time ON agent_metrics(agent_id, timestamp);

-- Error Logs
CREATE TABLE IF NOT EXISTS error_logs (
    error_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT,
    task_id TEXT,
    pipeline_id TEXT,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT 0,
    FOREIGN KEY (agent_id) REFERENCES agents(agent_id),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
);

CREATE INDEX idx_errors_timestamp ON error_logs(timestamp);
CREATE INDEX idx_errors_agent ON error_logs(agent_id);

-- Backpressure Events
CREATE TABLE IF NOT EXISTS backpressure_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    pressure_level TEXT NOT NULL CHECK(pressure_level IN ('normal', 'moderate', 'high', 'critical')),
    queue_length INTEGER,
    action_taken TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

-- Views for monitoring
CREATE VIEW IF NOT EXISTS agent_health AS
SELECT 
    a.agent_id,
    a.agent_type,
    a.status,
    a.last_heartbeat,
    CAST((julianday('now') - julianday(a.last_heartbeat)) * 86400 AS INTEGER) as seconds_since_heartbeat,
    COUNT(t.task_id) as active_tasks
FROM agents a
LEFT JOIN tasks t ON a.agent_id = t.agent_id AND t.status IN ('assigned', 'running')
GROUP BY a.agent_id;

CREATE VIEW IF NOT EXISTS pipeline_summary AS
SELECT 
    p.pipeline_id,
    p.status,
    COUNT(DISTINCT t.task_id) as total_tasks,
    SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
    SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
    p.created_at,
    p.completed_at
FROM pipelines p
LEFT JOIN tasks t ON p.pipeline_id = t.pipeline_id
GROUP BY p.pipeline_id;