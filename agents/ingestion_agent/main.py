"""
Ingestion Agent - Phase 2 Implementation
Captures frames from video files and sends to processing pipeline
"""
import asyncio
import base64
import cv2
import os
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from fastapi import HTTPException
from pydantic import BaseModel

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from base.base_agent import BaseAgent, TaskMessage


# Models
class IngestionJobConfig(BaseModel):
    source_id: str
    video_path: str
    fps: int = 30  # Target FPS for processing
    batch_size: int = 10
    start_frame: int = 0
    end_frame: Optional[int] = None
    resolution: Optional[Dict[str, int]] = None  # {width, height}


class IngestionJob:
    """Represents an active ingestion job"""
    def __init__(self, job_id: str, config: IngestionJobConfig):
        self.job_id = job_id
        self.config = config
        self.status = "initializing"
        self.frames_ingested = 0
        self.current_fps = 0
        self.task: Optional[asyncio.Task] = None
        self.stop_flag = asyncio.Event()
        self.cap: Optional[cv2.VideoCapture] = None


class IngestionAgent(BaseAgent):
    """
    Ingestion Agent - Reads video files and sends frames to processing
    """
    
    def __init__(self, agent_type: str = "ingestion", port: int = 8001, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        # Job management
        self.jobs: Dict[str, IngestionJob] = {}
        
        # Processing agent info (will be fetched from orchestrator)
        self.processing_agent_url: Optional[str] = None
    
    def setup_custom_routes(self):
        """Setup ingestion-specific routes"""
        
        @self.app.post("/ingest/start")
        async def start_ingestion(config: IngestionJobConfig):
            """Start video ingestion job"""
            try:
                # Validate video file exists
                if not Path(config.video_path).exists():
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Video file not found: {config.video_path}"
                    )
                
                # Create job
                job_id = str(uuid.uuid4())
                job = IngestionJob(job_id, config)
                self.jobs[job_id] = job
                
                # Start ingestion task
                job.task = asyncio.create_task(
                    self._run_ingestion(job)
                )
                
                self.logger.info(f"Started ingestion job: {job_id}")
                
                return {
                    "job_id": job_id,
                    "status": "started",
                    "config": config.model_dump()
                }
                
            except Exception as e:
                self.logger.error(f"Failed to start ingestion: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/ingest/{job_id}/stop")
        async def stop_ingestion(job_id: str):
            """Stop ingestion job"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            job.stop_flag.set()
            
            # Wait for task to complete
            if job.task:
                await job.task
            
            job.status = "stopped"
            
            return {"job_id": job_id, "status": "stopped"}
        
        @self.app.get("/ingest/{job_id}/status")
        async def get_status(job_id: str):
            """Get job status"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            return {
                "job_id": job_id,
                "status": job.status,
                "frames_ingested": job.frames_ingested,
                "current_fps": job.current_fps,
                "config": job.config.model_dump()
            }
        
        @self.app.patch("/ingest/{job_id}/config")
        async def update_config(job_id: str, fps: Optional[int] = None, batch_size: Optional[int] = None):
            """Update job config (for backpressure handling)"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            
            if fps is not None:
                job.config.fps = fps
                self.logger.info(f"Updated job {job_id} FPS to {fps}")
            
            if batch_size is not None:
                job.config.batch_size = batch_size
                self.logger.info(f"Updated job {job_id} batch_size to {batch_size}")
            
            return {
                "job_id": job_id,
                "fps": job.config.fps,
                "batch_size": job.config.batch_size
            }
    
    async def _run_ingestion(self, job: IngestionJob):
        """Main ingestion loop"""
        try:
            job.status = "running"
            
            # Get processing agent endpoint
            if not self.processing_agent_url:
                await self._discover_processing_agent()
            
            # Open video
            job.cap = cv2.VideoCapture(job.config.video_path)
            if not job.cap.isOpened():
                raise Exception(f"Failed to open video: {job.config.video_path}")
            
            # Get video properties
            video_fps = job.cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(job.cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            self.logger.info(f"Video: {video_fps} FPS, {total_frames} frames")
            
            # Set start frame
            if job.config.start_frame > 0:
                job.cap.set(cv2.CAP_PROP_POS_FRAMES, job.config.start_frame)
            
            # Calculate frame skip for target FPS
            frame_skip = max(1, int(video_fps / job.config.fps))
            
            batch = []
            frame_count = job.config.start_frame
            
            while not job.stop_flag.is_set():
                # Check end frame
                if job.config.end_frame and frame_count >= job.config.end_frame:
                    break
                
                # Read frame
                ret, frame = job.cap.read()
                if not ret:
                    break  # End of video
                
                frame_count += 1
                
                # Skip frames to match target FPS
                if (frame_count - job.config.start_frame) % frame_skip != 0:
                    continue
                
                # Resize if needed
                if job.config.resolution:
                    frame = cv2.resize(
                        frame,
                        (job.config.resolution['width'], job.config.resolution['height'])
                    )
                
                # Encode frame to JPEG
                _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                frame_b64 = base64.b64encode(buffer).decode('utf-8')
                
                # Add to batch
                batch.append({
                    "frame_id": f"{job.config.source_id}_{frame_count}",
                    "sequence_number": frame_count,
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": frame_b64,
                    "metadata": {
                        "width": frame.shape[1],
                        "height": frame.shape[0],
                        "format": "jpeg",
                        "size_bytes": len(buffer)
                    }
                })
                
                job.frames_ingested += 1
                
                # Send batch when full
                if len(batch) >= job.config.batch_size:
                    await self._send_batch(job, batch)
                    batch = []
                
                # Rate limiting to match target FPS
                await asyncio.sleep(1.0 / job.config.fps)
            
            # Send remaining frames
            if batch:
                await self._send_batch(job, batch)
            
            job.status = "completed"
            self.logger.info(f"Job {job.job_id} completed: {job.frames_ingested} frames")
            
        except Exception as e:
            job.status = "failed"
            self.logger.error(f"Ingestion job failed: {e}")
            raise
        finally:
            if job.cap:
                job.cap.release()
    
    async def _send_batch(self, job: IngestionJob, batch: list):
        """Send batch to processing agent"""
        if not self.processing_agent_url:
            self.logger.warning("No processing agent available")
            return
        
        try:
            batch_id = str(uuid.uuid4())
            
            payload = {
                "batch_id": batch_id,
                "source_id": job.config.source_id,
                "frames": batch
            }
            
            # Send via message
            await self.send_message(
                receiver="processing",
                message_type="process_batch",
                payload=payload,
                receiver_url=self.processing_agent_url
            )
            
            self.logger.info(f"Sent batch {batch_id} with {len(batch)} frames")
            
        except Exception as e:
            self.logger.error(f"Failed to send batch: {e}")
    
    async def _discover_processing_agent(self):
        """Discover processing agent from orchestrator"""
        if not self.orchestrator_url:
            return
        
        try:
            response = await self.http_client.get(
                f"{self.orchestrator_url}/agents"
            )
            response.raise_for_status()
            
            agents = response.json()['agents']
            
            # Find processing agent
            for agent in agents:
                if agent['agent_type'] == 'processing':
                    # Use localhost instead of container name
                    self.processing_agent_url = "http://processing:8002"
                    self.logger.info(f"Found processing agent: {self.processing_agent_url}")
                    return
            
            self.logger.warning("No processing agent found")
            
        except Exception as e:
            self.logger.error(f"Failed to discover processing agent: {e}")
    
    async def get_capabilities(self) -> list[str]:
        return ["video_ingestion", "frame_capture", "batch_processing"]
    
    async def on_startup(self):
        self.logger.info("Ingestion Agent started")
        # Discover processing agent
        if self.orchestrator_url:
            await asyncio.sleep(2)  # Wait for other agents to register
            await self._discover_processing_agent()
    
    async def on_shutdown(self):
        # Stop all jobs
        for job_id, job in self.jobs.items():
            job.stop_flag.set()
            if job.task:
                await job.task
        
        self.logger.info("Ingestion Agent stopped")


if __name__ == "__main__":
    agent = IngestionAgent(
        port=8001,
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000")
    )
    agent.run()