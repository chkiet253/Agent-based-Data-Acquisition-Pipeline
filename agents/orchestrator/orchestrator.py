"""
Orchestrator Agent - Central coordinator for the multi-agent pipeline
Phase 1: Basic registration and heartbeat functionality
"""
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

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


class AgentInfo(BaseModel):
    agent_id: str
    agent_type: str
    endpoint: str
    status: str
    capabilities: List[str]
    registered_at: datetime
    last_heartbeat: Optional[datetime]


class OrchestratorAgent:
    """
    Phase 1 Orchestrator: Handles agent registration and heartbeats
    """
    
    def __init__(self, db_path: str = "/data/orchestrator.db"):
        self.db_path = db_path
        self.app = FastAPI(title="Orchestrator Agent")
        self._init_database()
        self._setup_routes()
    
    def _init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create agents table
        cursor.execute("""
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
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def _get_db(self):
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        @self.app.get("/health")
        async def health():
            return {
                "status": "healthy",
                "agent_id": "orchestrator-001",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        @self.app.post("/agents/register")
        async def register_agent(registration: AgentRegistration):
            """Register a new agent"""
            try:
                conn = self._get_db()
                cursor = conn.cursor()
                
                # Generate agent ID
                agent_id = f"{registration.agent_type}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                
                # Insert agent
                cursor.execute("""
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
                
                conn.commit()
                conn.close()
                
                logger.info(f"Registered agent: {agent_id} ({registration.agent_type})")
                
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
                conn = self._get_db()
                cursor = conn.cursor()
                
                # Update last heartbeat and status
                cursor.execute("""
                    UPDATE agents 
                    SET last_heartbeat = ?, status = ?
                    WHERE agent_id = ?
                """, (datetime.utcnow(), heartbeat.status, agent_id))
                
                if cursor.rowcount == 0:
                    conn.close()
                    raise HTTPException(status_code=404, detail="Agent not found")
                
                conn.commit()
                conn.close()
                
                logger.debug(f"Heartbeat received from {agent_id}: {heartbeat.status}")
                
                return {"status": "acknowledged"}
                
            except Exception as e:
                logger.error(f"Heartbeat processing failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/agents")
        async def list_agents():
            """List all registered agents"""
            try:
                conn = self._get_db()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT agent_id, agent_type, endpoint, status, 
                           capabilities, registered_at, last_heartbeat
                    FROM agents
                    ORDER BY registered_at DESC
                """)
                
                agents = []
                for row in cursor.fetchall():
                    agents.append({
                        "agent_id": row["agent_id"],
                        "agent_type": row["agent_type"],
                        "endpoint": row["endpoint"],
                        "status": row["status"],
                        "capabilities": json.loads(row["capabilities"] or "[]"),
                        "registered_at": row["registered_at"],
                        "last_heartbeat": row["last_heartbeat"]
                    })
                
                conn.close()
                
                return {"agents": agents, "count": len(agents)}
                
            except Exception as e:
                logger.error(f"Failed to list agents: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/agents/{agent_id}")
        async def get_agent(agent_id: str):
            """Get specific agent info"""
            try:
                conn = self._get_db()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT agent_id, agent_type, endpoint, status, 
                           capabilities, registered_at, last_heartbeat, metadata
                    FROM agents
                    WHERE agent_id = ?
                """, (agent_id,))
                
                row = cursor.fetchone()
                conn.close()
                
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
                conn = self._get_db()
                cursor = conn.cursor()
                
                cursor.execute("DELETE FROM agents WHERE agent_id = ?", (agent_id,))
                
                if cursor.rowcount == 0:
                    conn.close()
                    raise HTTPException(status_code=404, detail="Agent not found")
                
                conn.commit()
                conn.close()
                
                logger.info(f"Unregistered agent: {agent_id}")
                
                return {"status": "unregistered", "agent_id": agent_id}
                
            except Exception as e:
                logger.error(f"Failed to unregister agent: {e}")
                raise HTTPException(status_code=500, detail=str(e))
    
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the orchestrator"""
        import uvicorn
        
        logger.info(f"Starting Orchestrator on {host}:{port}")
        uvicorn.run(self.app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    orchestrator = OrchestratorAgent()
    orchestrator.run()