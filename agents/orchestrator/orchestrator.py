# agents/orchestrator/orchestrator.py
"""
Orchestrator Agent - Updated for Phase 4
Now includes integrated dashboard
"""
import aiosqlite
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")


# Models
class AgentRegistration(BaseModel):
    agent_type: str
    endpoint: str
    capabilities: List[str] = []
    metadata: Dict = {}


class Heartbeat(BaseModel):
    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metrics: Dict = {}


class OrchestratorAgent:
    """
    Phase 4 Orchestrator with integrated dashboard
    """
    
    def __init__(self, db_path: str = "/data/orchestrator.db"):
        self.db_path = db_path
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.app = FastAPI(title="Orchestrator Agent")
        
        # Import dashboard - FIXED PATH
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        
        from dashboard import OrchestratorDashboard
        self.dashboard = OrchestratorDashboard(self)
        
        self._setup_routes()
        self.dashboard.setup_routes(self.app)
    
    async def _init_database(self):
        """Initialize SQLite database (async)"""
        async with aiosqlite.connect(self.db_path) as db:
            # Create agents table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    agent_type TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'idle',
                    capabilities TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_heartbeat TIMESTAMP,
                    metadata TEXT
                )
            """)
            await db.commit()
        
        logger.info(f"Database initialized at {self.db_path}")
    
    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        @self.app.on_event("startup")
        async def startup():
            await self._init_database()
            logger.info("🚀 Phase 4 Orchestrator started with Dashboard")
        
        @self.app.get("/health")
        async def health():
            return {
                "status": "healthy",
                "agent_id": "orchestrator-001",
                "timestamp": datetime.utcnow().isoformat(),
                "phase": "4-advanced"
            }
        
        @self.app.post("/agents/register")
        async def register_agent(registration: AgentRegistration):
            """Register a new agent"""
            try:
                # Generate agent ID
                agent_id = f"{registration.agent_type}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                
                # Insert agent (async)
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("""
                        INSERT INTO agents (agent_id, agent_type, endpoint, status, capabilities, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        agent_id,
                        registration.agent_type,
                        registration.endpoint,
                        'idle',
                        json.dumps(registration.capabilities),
                        json.dumps(registration.metadata)
                    ))
                    await db.commit()
                
                logger.info(f"✅ Registered agent: {agent_id} ({registration.agent_type})")
                
                return {
                    "agent_id": agent_id,
                    "status": "registered",
                    "message": f"Agent {agent_id} registered successfully"
                }
                
            except Exception as e:
                logger.error(f"Registration failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/agents/{agent_id}/heartbeat")
        async def receive_heartbeat(agent_id: str, heartbeat: Heartbeat):
            """Receive heartbeat from agent"""
            try:
                # Update last heartbeat and status (async)
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute("""
                        UPDATE agents 
                        SET last_heartbeat = ?, status = ?
                        WHERE agent_id = ?
                    """, (datetime.utcnow(), heartbeat.status, agent_id))
                    
                    if cursor.rowcount == 0:
                        raise HTTPException(status_code=404, detail="Agent not found")
                    
                    await db.commit()
                
                logger.debug(f"💓 Heartbeat from {agent_id}: {heartbeat.status}")
                
                return {"status": "acknowledged"}
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Heartbeat processing failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/agents")
        async def list_agents():
            """List all registered agents"""
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    cursor = await db.execute("""
                        SELECT agent_id, agent_type, endpoint, status, 
                               capabilities, registered_at, last_heartbeat
                        FROM agents
                        ORDER BY registered_at DESC
                    """)
                    
                    rows = await cursor.fetchall()
                    
                    agents = []
                    for row in rows:
                        agents.append({
                            "agent_id": row["agent_id"],
                            "agent_type": row["agent_type"],
                            "endpoint": row["endpoint"],
                            "status": row["status"],
                            "capabilities": json.loads(row["capabilities"] or "[]"),
                            "registered_at": row["registered_at"],
                            "last_heartbeat": row["last_heartbeat"]
                        })
                
                return {"agents": agents, "count": len(agents)}
                
            except Exception as e:
                logger.error(f"Failed to list agents: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/agents/{agent_id}")
        async def get_agent(agent_id: str):
            """Get specific agent info"""
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    cursor = await db.execute("""
                        SELECT agent_id, agent_type, endpoint, status, 
                               capabilities, registered_at, last_heartbeat, metadata
                        FROM agents
                        WHERE agent_id = ?
                    """, (agent_id,))
                    
                    row = await cursor.fetchone()
                    
                    if not row:
                        raise HTTPException(status_code=404, detail="Agent not found")
                    
                    return {
                        "agent_id": row["agent_id"],
                        "agent_type": row["agent_type"],
                        "endpoint": row["endpoint"],
                        "status": row["status"],
                        "capabilities": json.loads(row["capabilities"] or "[]"),
                        "registered_at": row["registered_at"],
                        "last_heartbeat": row["last_heartbeat"],
                        "metadata": json.loads(row["metadata"] or "{}")
                    }
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to get agent: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.delete("/agents/{agent_id}")
        async def unregister_agent(agent_id: str):
            """Unregister an agent"""
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute(
                        "DELETE FROM agents WHERE agent_id = ?", 
                        (agent_id,)
                    )
                    
                    if cursor.rowcount == 0:
                        raise HTTPException(status_code=404, detail="Agent not found")
                    
                    await db.commit()
                
                logger.info(f"Unregistered agent: {agent_id}")
                
                return {"status": "unregistered", "agent_id": agent_id}
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to unregister agent: {e}")
                raise HTTPException(status_code=500, detail=str(e))
    
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the orchestrator"""
        import uvicorn
        
        logger.info(f"🎯 Starting Phase 4 Orchestrator")
        logger.info(f"📊 Dashboard available at http://localhost:{port}/dashboard")
        uvicorn.run(self.app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    orchestrator = OrchestratorAgent()
    orchestrator.run()