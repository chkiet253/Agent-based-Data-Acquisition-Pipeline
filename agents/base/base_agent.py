"""
Base Agent Class - Foundation for all agents in the system
"""
import os
import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# Pydantic Models
class HealthResponse(BaseModel):
    status: str = "healthy"
    agent_id: str
    agent_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    uptime_seconds: float = 0


class AgentRegistration(BaseModel):
    agent_type: str
    endpoint: str
    capabilities: list[str] = []
    metadata: Dict[str, Any] = {}


class Heartbeat(BaseModel):
    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metrics: Dict[str, Any] = {}


class TaskMessage(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    message_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    sender: str
    receiver: str
    payload: Dict[str, Any] = {}
    correlation_id: Optional[str] = None


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the multi-agent system.
    Provides common functionality: registration, heartbeat, health checks.
    """
    
    def __init__(
        self,
        agent_type: str,
        agent_id: Optional[str] = None,
        port: int = 8000,
        orchestrator_url: Optional[str] = None
    ):
        self.agent_type = agent_type
        self.agent_id = agent_id or f"{agent_type}-{uuid.uuid4().hex[:8]}"
        self.port = port
        self.orchestrator_url = orchestrator_url or os.getenv("ORCHESTRATOR_URL")
        
        # Setup logging
        self.logger = logging.getLogger(self.agent_id)
        self.logger.setLevel(logging.INFO)
        
        # FastAPI app
        self.app = FastAPI(title=f"{agent_type.capitalize()} Agent")
        self._setup_routes()
        
        # State
        self.start_time = datetime.utcnow()
        self.status = "initializing"
        self.registered = False
        
        # Async HTTP client
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Background tasks
        self.heartbeat_task: Optional[asyncio.Task] = None
        
    def _setup_routes(self):
        """Setup common FastAPI routes"""
        
        @self.app.get("/health")
        async def health_check():
            uptime = (datetime.utcnow() - self.start_time).total_seconds()
            return HealthResponse(
                agent_id=self.agent_id,
                agent_type=self.agent_type,
                status=self.status,
                uptime_seconds=uptime
            )
        
        @self.app.post("/message")
        async def receive_message(message: TaskMessage):
            """Receive message from other agents"""
            self.logger.info(f"Received {message.message_type} from {message.sender}")
            return await self.handle_message(message)
        
        # Add custom routes
        self.setup_custom_routes()
    
    @abstractmethod
    def setup_custom_routes(self):
        """Subclasses implement their specific routes"""
        pass
    
    @abstractmethod
    async def get_capabilities(self) -> list[str]:
        """Return list of capabilities this agent provides"""
        pass
    
    async def handle_message(self, message: TaskMessage) -> Dict[str, Any]:
        """
        Handle incoming messages. Override in subclasses for specific logic.
        """
        self.logger.info(f"Base handler for message type: {message.message_type}")
        return {"status": "acknowledged", "message_id": message.message_id}
    
    async def register_with_orchestrator(self) -> bool:
        """Register this agent with the orchestrator"""
        if not self.orchestrator_url:
            self.logger.warning("No orchestrator URL configured")
            return False
        
        try:
            capabilities = await self.get_capabilities()
            registration = AgentRegistration(
                agent_type=self.agent_type,
                endpoint=f"http://{self.agent_id}:{self.port}",
                capabilities=capabilities,
                metadata={"version": "1.0.0"}
            )
            
            response = await self.http_client.post(
                f"{self.orchestrator_url}/agents/register",
                json=registration.model_dump()
            )
            response.raise_for_status()
            
            self.logger.info(f"Successfully registered with orchestrator")
            self.registered = True
            self.status = "idle"
            return True
            
        except Exception as e:
            self.logger.error(f"Registration failed: {e}")
            return False
    
    async def send_heartbeat(self):
        """Send periodic heartbeat to orchestrator"""
        if not self.orchestrator_url or not self.registered:
            return
        
        try:
            heartbeat = Heartbeat(
                status=self.status,
                metrics=await self.get_metrics()
            )
            
            response = await self.http_client.post(
                f"{self.orchestrator_url}/agents/{self.agent_id}/heartbeat",
                json=heartbeat.model_dump()
            )
            response.raise_for_status()
            
        except Exception as e:
            self.logger.error(f"Heartbeat failed: {e}")
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get current agent metrics. Override for custom metrics."""
        return {
            "uptime": (datetime.utcnow() - self.start_time).total_seconds(),
            "status": self.status
        }
    
    async def start_heartbeat_loop(self, interval: int = 30):
        """Start background heartbeat task"""
        while True:
            await asyncio.sleep(interval)
            await self.send_heartbeat()
    
    async def send_message(
        self,
        receiver: str,
        message_type: str,
        payload: Dict[str, Any],
        receiver_url: str
    ) -> Dict[str, Any]:
        """Send message to another agent"""
        message = TaskMessage(
            message_type=message_type,
            sender=self.agent_id,
            receiver=receiver,
            payload=payload
        )
        
        try:
            response = await self.http_client.post(
                f"{receiver_url}/message",
                json=message.model_dump()
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Failed to send message to {receiver}: {e}")
            raise
    
    async def startup(self):
        """Startup sequence"""
        self.logger.info(f"Starting {self.agent_type} agent: {self.agent_id}")
        
        # Register with orchestrator
        if self.orchestrator_url:
            registered = await self.register_with_orchestrator()
            if registered:
                # Start heartbeat
                self.heartbeat_task = asyncio.create_task(
                    self.start_heartbeat_loop()
                )
        
        # Custom startup logic
        await self.on_startup()
    
    async def shutdown(self):
        """Shutdown sequence"""
        self.logger.info(f"Shutting down {self.agent_id}")
        
        # Cancel heartbeat
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        # Close HTTP client
        await self.http_client.aclose()
        
        # Custom shutdown logic
        await self.on_shutdown()
    
    @abstractmethod
    async def on_startup(self):
        """Custom startup logic for subclasses"""
        pass
    
    @abstractmethod
    async def on_shutdown(self):
        """Custom shutdown logic for subclasses"""
        pass
    
    def run(self):
        """Run the agent"""
        import uvicorn
        
        @self.app.on_event("startup")
        async def startup_event():
            await self.startup()
        
        @self.app.on_event("shutdown")
        async def shutdown_event():
            await self.shutdown()
        
        uvicorn.run(
            self.app,
            host="0.0.0.0",
            port=self.port,
            log_level="info"
        )


# Example: Simple Hello World Agent
class HelloWorldAgent(BaseAgent):
    """Example agent for testing"""
    
    def setup_custom_routes(self):
        @self.app.get("/hello")
        async def hello():
            return {"message": f"Hello from {self.agent_id}!"}
    
    async def get_capabilities(self) -> list[str]:
        return ["hello", "echo"]
    
    async def on_startup(self):
        self.logger.info("HelloWorldAgent started!")
    
    async def on_shutdown(self):
        self.logger.info("HelloWorldAgent stopped!")


if __name__ == "__main__":
    # Test the base agent
    agent = HelloWorldAgent(
        agent_type="hello",
        port=8001,
        orchestrator_url="http://localhost:8000"
    )
    agent.run()