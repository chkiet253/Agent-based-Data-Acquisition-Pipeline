"""
Orchestrator Dashboard - Simple web interface for monitoring
Phase 4: Shows real-time metrics without separate monitoring agent
"""
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from datetime import datetime
import httpx
import logging

logger = logging.getLogger("orchestrator.dashboard")


class OrchestratorDashboard:
    """
    Simple web dashboard integrated into Orchestrator
    No separate monitoring agent needed
    """
    
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.http_client = httpx.AsyncClient(timeout=10.0)
    
    def setup_routes(self, app: FastAPI):
        """Add dashboard routes to orchestrator app"""
        
        @app.get("/dashboard", response_class=HTMLResponse)
        async def dashboard():
            """Serve monitoring dashboard"""
            return self._get_dashboard_html()
        
        @app.get("/api/metrics/summary")
        async def metrics_summary():
            """API endpoint for dashboard data"""
            return await self.collect_all_metrics()
        
        @app.get("/api/metrics/history")
        async def metrics_history():
            """Get historical metrics (simple in-memory)"""
            return {
                "history": getattr(self, 'metrics_history', []),
                "window_minutes": 10
            }
    
    async def collect_all_metrics(self):
        """
        Collect metrics from all registered agents
        """
        try:
            # Get all agents from orchestrator DB
            agents_status = await self._get_agents_status()
            
            # Query each agent type for metrics
            ingestion_metrics = []
            processing_metrics = []
            storage_metrics = []
            
            for agent in agents_status:
                try:
                    # Try to get metrics from agent
                    if agent['agent_type'] == 'ingestion':
                        metrics = await self._query_ingestion_metrics()
                        ingestion_metrics.append(metrics)
                    elif agent['agent_type'] == 'processing':
                        metrics = await self._query_processing_metrics()
                        processing_metrics.append(metrics)
                    elif agent['agent_type'] == 'storage':
                        metrics = await self._query_storage_metrics()
                        storage_metrics.append(metrics)
                except Exception as e:
                    logger.warning(f"Failed to get metrics from {agent['agent_id']}: {e}")
            
            # Aggregate metrics
            pipeline_metrics = self._aggregate_pipeline_metrics(
                ingestion_metrics,
                processing_metrics,
                storage_metrics
            )
            
            # Autonomy metrics
            autonomy_metrics = self._extract_autonomy_metrics(
                ingestion_metrics,
                processing_metrics,
                storage_metrics
            )
            
            # Queue status
            queue_metrics = self._extract_queue_metrics(
                processing_metrics,
                storage_metrics
            )
            
            result = {
                'timestamp': datetime.utcnow().isoformat(),
                'agents': agents_status,
                'pipeline': pipeline_metrics,
                'queues': queue_metrics,
                'autonomy': autonomy_metrics
            }
            
            # Store in history (keep last 100)
            if not hasattr(self, 'metrics_history'):
                self.metrics_history = []
            
            self.metrics_history.append(result)
            if len(self.metrics_history) > 100:
                self.metrics_history.pop(0)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to collect metrics: {e}")
            return self._get_empty_metrics()
    
    async def _get_agents_status(self):
        """Get status of all agents"""
        import aiosqlite
        
        agents = []
        async with aiosqlite.connect(self.orchestrator.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT agent_id, agent_type, status, last_heartbeat, endpoint
                FROM agents
                ORDER BY agent_type, registered_at
            """)
            
            rows = await cursor.fetchall()
            
            for row in rows:
                # Calculate health
                health = "healthy"
                if row["last_heartbeat"]:
                    import sqlite3
                    from datetime import datetime, timedelta
                    
                    try:
                        last_hb = datetime.fromisoformat(row["last_heartbeat"])
                        age = (datetime.utcnow() - last_hb).total_seconds()
                        
                        if age > 120:
                            health = "critical"
                        elif age > 60:
                            health = "degraded"
                    except:
                        health = "unknown"
                else:
                    health = "unknown"
                
                agents.append({
                    "type": row["agent_type"],
                    "id": row["agent_id"],
                    "status": row["status"],
                    "health": health,
                    "last_heartbeat": row["last_heartbeat"] or "Never",
                    "endpoint": row["endpoint"]
                })
        
        return agents
    
    async def _query_ingestion_metrics(self):
        """Query ingestion agent metrics"""
        try:
            response = await self.http_client.get(
                "http://ingestion:8001/ingest/status",
                timeout=5.0
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.debug(f"Ingestion metrics unavailable: {e}")
        
        return {}
    
    async def _query_processing_metrics(self):
        """Query processing agent metrics"""
        try:
            response = await self.http_client.get(
                "http://processing:8002/process/status",
                timeout=5.0
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.debug(f"Processing metrics unavailable: {e}")
        
        return {}
    
    async def _query_storage_metrics(self):
        """Query storage agent metrics"""
        try:
            response = await self.http_client.get(
                "http://storage:8003/storage/status",
                timeout=5.0
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.debug(f"Storage metrics unavailable: {e}")
        
        return {}
    
    def _aggregate_pipeline_metrics(self, ingestion, processing, storage):
        """Aggregate pipeline metrics"""
        total_ingested = sum(m.get('frames_ingested', 0) for m in ingestion)
        total_processed = sum(m.get('processed_frames', 0) for m in processing)
        total_stored = sum(m.get('frames_stored', 0) for m in storage)
        total_detections = sum(m.get('detection_count', 0) for m in processing)
        
        return {
            'ingested': total_ingested,
            'processed': total_processed,
            'stored': total_stored,
            'detections': total_detections
        }
    
    def _extract_autonomy_metrics(self, ingestion, processing, storage):
        """Extract autonomy-related metrics"""
        current_fps = ingestion[0].get('current_fps', 0) if ingestion else 0
        retry_count = sum(m.get('retry_count', 0) for m in processing)
        storage_mode = storage[0].get('storage_mode', 'N/A') if storage else 'N/A'
        circuit_state = processing[0].get('circuit_breaker_state', 'N/A') if processing else 'N/A'
        
        return {
            'current_fps': current_fps,
            'retry_count': retry_count,
            'storage_mode': storage_mode,
            'circuit_state': circuit_state
        }
    
    def _extract_queue_metrics(self, processing, storage):
        """Extract queue metrics"""
        processing_queue = processing[0].get('queue_length', 0) if processing else 0
        storage_queue = storage[0].get('queue_length', 0) if storage else 0
        
        return {
            'processing': processing_queue,
            'storage': storage_queue
        }
    
    def _get_empty_metrics(self):
        """Return empty metrics structure"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'agents': [],
            'pipeline': {'ingested': 0, 'processed': 0, 'stored': 0, 'detections': 0},
            'queues': {'processing': 0, 'storage': 0},
            'autonomy': {
                'current_fps': 0,
                'retry_count': 0,
                'storage_mode': 'N/A',
                'circuit_state': 'N/A'
            }
        }
    
    def _get_dashboard_html(self):
        """Generate dashboard HTML"""
        return """
<!DOCTYPE html>
<html>
<head>
    <title>Multi-Agent Pipeline Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        
        .container { max-width: 1400px; margin: 0 auto; }
        
        h1 { 
            color: white; 
            text-align: center; 
            margin-bottom: 30px;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .card {
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        
        .card:hover { transform: translateY(-2px); }
        
        .card h2 {
            color: #333;
            margin-bottom: 20px;
            font-size: 1.3em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        
        .metric {
            display: inline-block;
            margin: 10px 20px 10px 0;
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .metric-label {
            color: #666;
            font-size: 0.9em;
            margin-top: 5px;
        }
        
        .agent-card {
            display: inline-block;
            margin: 10px;
            padding: 15px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            min-width: 200px;
        }
        
        .agent-type {
            font-weight: bold;
            font-size: 1.1em;
            color: #333;
            margin-bottom: 8px;
        }
        
        .agent-id {
            color: #666;
            font-size: 0.85em;
            margin-bottom: 8px;
        }
        
        .status-healthy { 
            color: #4CAF50; 
            font-weight: bold;
        }
        .status-degraded { 
            color: #FF9800; 
            font-weight: bold;
        }
        .status-critical { 
            color: #F44336; 
            font-weight: bold;
        }
        .status-unknown { 
            color: #9E9E9E; 
            font-weight: bold;
        }
        
        .chart-container {
            position: relative;
            height: 250px;
            margin-top: 20px;
        }
        
        .loading {
            text-align: center;
            color: #666;
            padding: 20px;
        }
        
        .last-update {
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 0.9em;
            opacity: 0.9;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
            margin-left: 10px;
        }
        
        .badge-primary { background: #2196F3; color: white; }
        .badge-fallback { background: #FF9800; color: white; }
        .badge-closed { background: #4CAF50; color: white; }
        .badge-open { background: #F44336; color: white; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Multi-Agent Pipeline Dashboard</h1>
        
        <!-- Agent Status -->
        <div class="card">
            <h2>🤖 Agent Status</h2>
            <div id="agent-status" class="loading">Loading agents...</div>
        </div>
        
        <div class="grid">
            <!-- Pipeline Metrics -->
            <div class="card">
                <h2>📊 Pipeline Metrics</h2>
                <div class="metric">
                    <div class="metric-value" id="frames-ingested">0</div>
                    <div class="metric-label">Frames Ingested</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="frames-processed">0</div>
                    <div class="metric-label">Frames Processed</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="frames-stored">0</div>
                    <div class="metric-label">Frames Stored</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="detections">0</div>
                    <div class="metric-label">Total Detections</div>
                </div>
            </div>
            
            <!-- Autonomy Features -->
            <div class="card">
                <h2>🧠 Autonomy Features</h2>
                <div class="metric">
                    <div class="metric-value" id="current-fps">30</div>
                    <div class="metric-label">Current FPS (Adaptive)</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="retry-count">0</div>
                    <div class="metric-label">Self-Healing Retries</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="storage-mode">PRIMARY</div>
                    <div class="metric-label">Storage Mode</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="circuit-state">CLOSED</div>
                    <div class="metric-label">Circuit Breaker</div>
                </div>
            </div>
        </div>
        
        <!-- Queue Status -->
        <div class="card">
            <h2>📦 Queue Status</h2>
            <div class="chart-container">
                <canvas id="queueChart"></canvas>
            </div>
        </div>
        
        <!-- Throughput Chart -->
        <div class="card">
            <h2>⚡ Pipeline Throughput (Last 10 min)</h2>
            <div class="chart-container">
                <canvas id="throughputChart"></canvas>
            </div>
        </div>
        
        <div class="last-update" id="last-update">
            Last updated: Never
        </div>
    </div>
    
    <script>
        let queueChart = null;
        let throughputChart = null;
        let throughputData = [];
        
        async function updateMetrics() {
            try {
                const response = await fetch('/api/metrics/summary');
                const data = await response.json();
                
                // Update timestamp
                document.getElementById('last-update').textContent = 
                    `Last updated: ${new Date(data.timestamp).toLocaleTimeString()}`;
                
                // Update agent status
                updateAgentStatus(data.agents);
                
                // Update pipeline metrics
                updatePipelineMetrics(data.pipeline);
                
                // Update autonomy metrics
                updateAutonomyMetrics(data.autonomy);
                
                // Update queue chart
                updateQueueChart(data.queues);
                
                // Update throughput chart
                updateThroughputChart(data.pipeline);
                
            } catch (e) {
                console.error('Failed to update metrics:', e);
                document.getElementById('last-update').textContent = 
                    `Error: ${e.message}`;
            }
        }
        
        function updateAgentStatus(agents) {
            if (agents.length === 0) {
                document.getElementById('agent-status').innerHTML = 
                    '<div class="loading">No agents registered</div>';
                return;
            }
            
            const html = agents.map(a => `
                <div class="agent-card">
                    <div class="agent-type">🤖 ${a.type.toUpperCase()}</div>
                    <div class="agent-id">${a.id}</div>
                    <div>Status: <span class="status-${a.health}">${a.status.toUpperCase()}</span></div>
                    <div style="font-size: 0.85em; color: #666; margin-top: 5px;">
                        Last HB: ${a.last_heartbeat === 'Never' ? 'Never' : new Date(a.last_heartbeat).toLocaleTimeString()}
                    </div>
                </div>
            `).join('');
            
            document.getElementById('agent-status').innerHTML = html;
        }
        
        function updatePipelineMetrics(pipeline) {
            document.getElementById('frames-ingested').textContent = pipeline.ingested.toLocaleString();
            document.getElementById('frames-processed').textContent = pipeline.processed.toLocaleString();
            document.getElementById('frames-stored').textContent = pipeline.stored.toLocaleString();
            document.getElementById('detections').textContent = pipeline.detections.toLocaleString();
        }
        
        function updateAutonomyMetrics(autonomy) {
            document.getElementById('current-fps').textContent = autonomy.current_fps;
            document.getElementById('retry-count').textContent = autonomy.retry_count;
            
            const storageMode = document.getElementById('storage-mode');
            storageMode.textContent = autonomy.storage_mode;
            
            const circuitState = document.getElementById('circuit-state');
            circuitState.textContent = autonomy.circuit_state;
        }
        
        function updateQueueChart(queues) {
            const ctx = document.getElementById('queueChart').getContext('2d');
            
            if (queueChart) {
                queueChart.data.datasets[0].data = [
                    queues.processing, 
                    queues.storage
                ];
                queueChart.update('none');
            } else {
                queueChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: ['Processing Queue', 'Storage Queue'],
                        datasets: [{
                            label: 'Queue Length',
                            data: [queues.processing, queues.storage],
                            backgroundColor: [
                                'rgba(102, 126, 234, 0.8)',
                                'rgba(118, 75, 162, 0.8)'
                            ],
                            borderColor: [
                                'rgba(102, 126, 234, 1)',
                                'rgba(118, 75, 162, 1)'
                            ],
                            borderWidth: 2
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { 
                                beginAtZero: true,
                                title: {
                                    display: true,
                                    text: 'Items in Queue'
                                }
                            }
                        },
                        plugins: {
                            legend: { display: false }
                        }
                    }
                });
            }
        }
        
        function updateThroughputChart(pipeline) {
            // Add current throughput to history
            throughputData.push({
                time: new Date().toLocaleTimeString(),
                frames: pipeline.processed
            });
            
            // Keep only last 30 data points
            if (throughputData.length > 30) {
                throughputData.shift();
            }
            
            const ctx = document.getElementById('throughputChart').getContext('2d');
            
            if (throughputChart) {
                throughputChart.data.labels = throughputData.map(d => d.time);
                throughputChart.data.datasets[0].data = throughputData.map(d => d.frames);
                throughputChart.update('none');
            } else {
                throughputChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: throughputData.map(d => d.time),
                        datasets: [{
                            label: 'Frames Processed',
                            data: throughputData.map(d => d.frames),
                            borderColor: 'rgba(102, 126, 234, 1)',
                            backgroundColor: 'rgba(102, 126, 234, 0.1)',
                            borderWidth: 2,
                            tension: 0.4,
                            fill: true
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { 
                                beginAtZero: true,
                                title: {
                                    display: true,
                                    text: 'Total Frames'
                                }
                            }
                        }
                    }
                });
            }
        }
        
        // Initial load
        updateMetrics();
        
        // Auto-refresh every 2 seconds
        setInterval(updateMetrics, 2000);
    </script>
</body>
</html>
        """