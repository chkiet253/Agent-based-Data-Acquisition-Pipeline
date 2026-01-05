"""
CRITICAL FIX: Dashboard với agent detection đúng
"""
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from datetime import datetime
import httpx
import logging

logger = logging.getLogger("orchestrator.dashboard")


class OrchestratorDashboard:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.http_client = httpx.AsyncClient(timeout=10.0)
    
    def setup_routes(self, app: FastAPI):
        @app.get("/dashboard", response_class=HTMLResponse)
        async def dashboard():
            return self._get_dashboard_html()
        
        @app.get("/api/metrics/summary")
        async def metrics_summary():
            return await self.collect_all_metrics()
    
    async def collect_all_metrics(self):
        """FIXED: Better agent detection"""
        try:
            # Get agents from database
            agents_status = await self._get_agents_status()
            
            logger.info(f"📊 Found {len(agents_status)} total agent records")
            
            # ✅ FIX: Filter by recent heartbeat (last 2 minutes)
            now = datetime.utcnow()
            active_agents = []
            
            for agent in agents_status:
                if agent.get('last_heartbeat'):
                    try:
                        hb_time = datetime.fromisoformat(agent['last_heartbeat'])
                        age_seconds = (now - hb_time).total_seconds()
                        
                        # Consider active if heartbeat within 2 minutes
                        if age_seconds < 120:
                            active_agents.append(agent)
                            logger.info(f"  ✅ Active: {agent['type']} (heartbeat {age_seconds:.0f}s ago)")
                        else:
                            logger.warning(f"  ⏰ Stale: {agent['type']} (heartbeat {age_seconds:.0f}s ago)")
                    except Exception as e:
                        logger.error(f"  ❌ Parse error for {agent.get('type')}: {e}")
            
            # ✅ FIX: Deduplicate by type, keep most recent
            latest_agents = {}
            for agent in active_agents:
                agent_type = agent['type']
                if agent_type not in latest_agents:
                    latest_agents[agent_type] = agent
                else:
                    current = latest_agents[agent_type]
                    if agent['last_heartbeat'] > current['last_heartbeat']:
                        latest_agents[agent_type] = agent
            
            agents_status = list(latest_agents.values())
            
            logger.info(f"📊 Using {len(agents_status)} active agents")
            
            if len(agents_status) == 0:
                logger.error("❌ NO ACTIVE AGENTS FOUND!")
                logger.error("   This means either:")
                logger.error("   1. Agents are not sending heartbeats")
                logger.error("   2. Database has stale data")
                logger.error("   3. Heartbeat threshold (120s) too strict")
            
            # Query metrics from agents
            ingestion_metrics = await self._query_agent_metrics("http://ingestion:8001/ingest/status")
            processing_metrics = await self._query_agent_metrics("http://processing:8002/process/status")
            storage_metrics = await self._query_agent_metrics("http://storage:8003/storage/status")
            
            # Aggregate pipeline metrics
            pipeline_metrics = {
                'ingested': ingestion_metrics.get('frames_ingested', 0) if ingestion_metrics else 0,
                'processed': processing_metrics.get('processed_frames', 0) if processing_metrics else 0,
                'stored': storage_metrics.get('frames_stored', 0) if storage_metrics else 0,
                'detections': processing_metrics.get('detection_count', 0) if processing_metrics else 0
            }
            
            # Queue metrics
            queue_metrics = {
                'processing': processing_metrics.get('queue_length', 0) if processing_metrics else 0,
                'storage': storage_metrics.get('queue_length', 0) if storage_metrics else 0
            }
            
            # Autonomy metrics
            autonomy_metrics = {
                'current_fps': ingestion_metrics.get('current_fps', 0) if ingestion_metrics else 0,
                'retry_count': processing_metrics.get('retry_count', 0) if processing_metrics else 0,
                'storage_mode': storage_metrics.get('storage_mode', 'N/A') if storage_metrics else 'N/A',
                'circuit_state': processing_metrics.get('circuit_breaker_state', 'N/A') if processing_metrics else 'N/A'
            }
            
            result = {
                'timestamp': datetime.utcnow().isoformat(),
                'agents': agents_status,
                'pipeline': pipeline_metrics,
                'queues': queue_metrics,
                'autonomy': autonomy_metrics
            }
            
            logger.info(f"✅ Metrics: {len(agents_status)} agents, {pipeline_metrics['processed']} processed")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to collect metrics: {e}", exc_info=True)
            return self._get_empty_metrics()
    
    async def _get_agents_status(self):
        """Get ALL agents from database (no filtering yet)"""
        import aiosqlite
        
        agents = []
        try:
            async with aiosqlite.connect(self.orchestrator.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("""
                    SELECT agent_id, agent_type, status, last_heartbeat, endpoint
                    FROM agents
                    ORDER BY registered_at DESC
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    # Calculate health
                    health = "unknown"
                    if row["last_heartbeat"]:
                        try:
                            last_hb = datetime.fromisoformat(row["last_heartbeat"])
                            age = (datetime.utcnow() - last_hb).total_seconds()
                            
                            if age > 120:
                                health = "critical"
                            elif age > 60:
                                health = "degraded"
                            else:
                                health = "healthy"
                        except:
                            health = "unknown"
                    
                    agents.append({
                        "type": row["agent_type"],
                        "id": row["agent_id"],
                        "status": row["status"],
                        "health": health,
                        "last_heartbeat": row["last_heartbeat"] or "Never",
                        "endpoint": row["endpoint"]
                    })
            
            logger.info(f"Retrieved {len(agents)} agents from database")
            
        except Exception as e:
            logger.error(f"Failed to get agents: {e}", exc_info=True)
        
        return agents
    
    async def _query_agent_metrics(self, url: str):
        """Query metrics from an agent"""
        try:
            response = await self.http_client.get(url, timeout=5.0)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.debug(f"Failed to query {url}: {e}")
        return None
    
    def _get_empty_metrics(self):
        """Empty metrics structure"""
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
        """Dashboard HTML (giữ nguyên)"""
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
        }
        
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
        
        .status-healthy { color: #4CAF50; font-weight: bold; }
        .status-degraded { color: #FF9800; font-weight: bold; }
        .status-critical { color: #F44336; font-weight: bold; }
        
        .chart-container {
            position: relative;
            height: 250px;
            margin-top: 20px;
        }
        
        .last-update {
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 0.9em;
        }
        
        /* Debug info */
        .debug-info {
            background: rgba(255,255,255,0.1);
            color: white;
            padding: 10px;
            border-radius: 8px;
            font-family: monospace;
            font-size: 0.85em;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Multi-Agent Pipeline Dashboard</h1>
        
        <div class="card">
            <h2>🤖 Agent Status</h2>
            <div id="agent-status">Loading...</div>
        </div>
        
        <div class="grid">
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
            
            <div class="card">
                <h2>🧠 Autonomy Features</h2>
                <div class="metric">
                    <div class="metric-value" id="current-fps">0</div>
                    <div class="metric-label">Current FPS</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="retry-count">0</div>
                    <div class="metric-label">Retries</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="storage-mode">N/A</div>
                    <div class="metric-label">Storage Mode</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="circuit-state">N/A</div>
                    <div class="metric-label">Circuit Breaker</div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>📦 Queue Status</h2>
            <div class="chart-container">
                <canvas id="queueChart"></canvas>
            </div>
        </div>
        
        <div class="card">
            <h2>⚡ Pipeline Throughput</h2>
            <div class="chart-container">
                <canvas id="throughputChart"></canvas>
            </div>
        </div>
        
        <div class="last-update" id="last-update">Last updated: Never</div>
        
        <!-- Debug info -->
        <div class="debug-info" id="debug-info">
            Debug: Waiting for data...
        </div>
    </div>
    
    <script>
        let queueChart, throughputChart;
        let throughputData = [];
        
        async function updateMetrics() {
            try {
                const response = await fetch('/api/metrics/summary');
                const data = await response.json();
                
                console.log('Metrics received:', data);
                
                // Update debug info
                document.getElementById('debug-info').textContent = 
                    `Debug: ${data.agents.length} agents, ` +
                    `${data.pipeline.processed} processed, ` +
                    `Last update: ${new Date(data.timestamp).toLocaleTimeString()}`;
                
                document.getElementById('last-update').textContent = 
                    `Last updated: ${new Date(data.timestamp).toLocaleTimeString()}`;
                
                updateAgentStatus(data.agents);
                updatePipelineMetrics(data.pipeline);
                updateAutonomyMetrics(data.autonomy);
                updateQueueChart(data.queues);
                updateThroughputChart(data.pipeline);
                
            } catch (e) {
                console.error('Failed to update metrics:', e);
                document.getElementById('debug-info').textContent = `Error: ${e.message}`;
            }
        }
        
        function updateAgentStatus(agents) {
            const container = document.getElementById('agent-status');
            
            if (!agents || agents.length === 0) {
                container.innerHTML = '<div style="color: #666; padding: 20px;">⚠️ No active agents (check heartbeats)</div>';
                return;
            }
            
            const html = agents.map(a => `
                <div class="agent-card">
                    <div class="agent-type">🤖 ${a.type.toUpperCase()}</div>
                    <div style="font-size: 0.85em; color: #666;">${a.id}</div>
                    <div>Status: <span class="status-${a.health}">${a.status.toUpperCase()}</span></div>
                    <div style="font-size: 0.8em; color: #999;">HB: ${a.last_heartbeat}</div>
                </div>
            `).join('');
            
            container.innerHTML = html;
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
            document.getElementById('storage-mode').textContent = autonomy.storage_mode;
            document.getElementById('circuit-state').textContent = autonomy.circuit_state;
        }
        
        function updateQueueChart(queues) {
            const ctx = document.getElementById('queueChart').getContext('2d');
            
            if (queueChart) {
                queueChart.data.datasets[0].data = [queues.processing, queues.storage];
                queueChart.update('none');
            } else {
                queueChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: ['Processing Queue', 'Storage Queue'],
                        datasets: [{
                            label: 'Queue Length',
                            data: [queues.processing, queues.storage],
                            backgroundColor: ['rgba(102, 126, 234, 0.8)', 'rgba(118, 75, 162, 0.8)']
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }
        }
        
        function updateThroughputChart(pipeline) {
            throughputData.push({
                time: new Date().toLocaleTimeString(),
                frames: pipeline.processed
            });
            
            if (throughputData.length > 30) throughputData.shift();
            
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
                            tension: 0.4,
                            fill: true
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }
        }
        
        updateMetrics();
        setInterval(updateMetrics, 2000);
    </script>
</body>
</html>
        """