"""
Phase 3: Ingestion Agent with Autonomy Features + HLS Stream Support
- Backpressure handling (adaptive FPS)
- Self-healing (retry logic)
- Adaptive batch sizing
- Queue monitoring
- HLS/RTSP/HTTP stream support
"""
import asyncio
import base64
import cv2
import os
import uuid
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from collections import deque

from fastapi import HTTPException
from pydantic import BaseModel

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from base.base_agent import BaseAgent, TaskMessage


# Models
class IngestionJobConfig(BaseModel):
    source_id: str
    video_path: str
    fps: int = 30
    batch_size: int = 10
    start_frame: int = 0
    end_frame: Optional[int] = None
    resolution: Optional[Dict[str, int]] = None
    # Phase 3: Adaptive configs
    min_fps: int = 5
    max_fps: int = 45
    adaptive_mode: bool = True


class BackpressureMetrics(BaseModel):
    queue_length: int
    avg_processing_time: float
    pressure_level: str  # normal, moderate, high, critical
    suggested_fps: int
    suggested_batch_size: int


class IngestionJob:
    """Enhanced ingestion job with autonomy features"""
    def __init__(self, job_id: str, config: IngestionJobConfig):
        self.job_id = job_id
        self.config = config
        self.status = "initializing"
        self.frames_ingested = 0
        self.frames_dropped = 0
        self.current_fps = config.fps
        self.current_batch_size = config.batch_size
        self.task: Optional[asyncio.Task] = None
        self.stop_flag = asyncio.Event()
        self.pause_flag = asyncio.Event()
        self.cap: Optional[cv2.VideoCapture] = None
        
        # Phase 3: Autonomy features
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.retry_count = 0
        self.max_retries = 3
        self.backoff_time = 1.0  # Exponential backoff
        self.processing_times = deque(maxlen=20)  # Track last 20 processing times
        self.last_adjustment_time = time.time()
        self.adjustment_cooldown = 10  # Seconds between adjustments
        
        # Stream handling
        self.is_live_stream = False
        self.stream_reconnect_count = 0
        self.max_reconnect_attempts = 5


class IngestionAgent(BaseAgent):
    """
    Phase 3: Autonomous Ingestion Agent with HLS/Stream Support
    - Adapts FPS based on processing queue
    - Self-heals on errors
    - Monitors and reports backpressure
    - Supports HLS, RTSP, HTTP streams
    """
    
    def __init__(self, agent_type: str = "ingestion", port: int = 8001, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        self.jobs: Dict[str, IngestionJob] = {}
        self.processing_agent_url: Optional[str] = None
        
        # Phase 3: Backpressure monitoring
        self.backpressure_threshold = 50  # Queue length threshold
        self.monitoring_task: Optional[asyncio.Task] = None
    
    def setup_custom_routes(self):
        """Setup ingestion-specific routes"""
        
        @self.app.post("/ingest/start")
        async def start_ingestion(config: IngestionJobConfig):
            try:
                # Validate video path/URL
                if not self._validate_source(config.video_path):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Invalid video source: {config.video_path}"
                    )
                
                job_id = str(uuid.uuid4())
                job = IngestionJob(job_id, config)
                self.jobs[job_id] = job
                
                job.task = asyncio.create_task(self._run_ingestion(job))
                
                self.logger.info(f"Started ingestion job: {job_id} (adaptive: {config.adaptive_mode})")
                
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
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            job.stop_flag.set()
            
            if job.task:
                await job.task
            
            job.status = "stopped"
            return {"job_id": job_id, "status": "stopped"}
        
        @self.app.post("/ingest/{job_id}/pause")
        async def pause_ingestion(job_id: str):
            """Phase 3: Pause/resume capability"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            
            if job.pause_flag.is_set():
                job.pause_flag.clear()
                self.logger.info(f"Resumed job {job_id}")
                return {"job_id": job_id, "status": "resumed"}
            else:
                job.pause_flag.set()
                self.logger.info(f"Paused job {job_id}")
                return {"job_id": job_id, "status": "paused"}
        
        @self.app.get("/ingest/{job_id}/status")
        async def get_status(job_id: str):
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            return {
                "job_id": job_id,
                "status": job.status,
                "frames_ingested": job.frames_ingested,
                "frames_dropped": job.frames_dropped,
                "current_fps": job.current_fps,
                "current_batch_size": job.current_batch_size,
                "queue_length": job.queue.qsize(),
                "retry_count": job.retry_count,
                "is_live_stream": job.is_live_stream,
                "config": job.config.model_dump()
            }
        
        @self.app.get("/ingest/{job_id}/backpressure")
        async def get_backpressure(job_id: str):
            """Phase 3: Backpressure metrics"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            metrics = self._calculate_backpressure(job)
            
            return metrics.model_dump()
        
        @self.app.patch("/ingest/{job_id}/config")
        async def update_config(
            job_id: str, 
            fps: Optional[int] = None, 
            batch_size: Optional[int] = None
        ):
            """Manual config override"""
            if job_id not in self.jobs:
                raise HTTPException(status_code=404, detail="Job not found")
            
            job = self.jobs[job_id]
            
            if fps is not None:
                job.current_fps = max(job.config.min_fps, min(fps, job.config.max_fps))
                self.logger.info(f"Manually set job {job_id} FPS to {job.current_fps}")
            
            if batch_size is not None:
                job.current_batch_size = max(1, min(batch_size, 50))
                self.logger.info(f"Manually set job {job_id} batch_size to {job.current_batch_size}")
            
            return {
                "job_id": job_id,
                "fps": job.current_fps,
                "batch_size": job.current_batch_size
            }
    
    def _validate_source(self, video_path: str) -> bool:
        """Validate video source (file or stream URL)"""
        # Check if it's a URL (stream)
        if video_path.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
            return True
        
        # Check if it's a file path
        if video_path.startswith('/'):
            # Absolute path in container
            return True
        
        # Check local file
        return Path(video_path).exists()
    
    def _is_stream_url(self, video_path: str) -> bool:
        """Check if source is a stream URL"""
        return video_path.startswith(('http://', 'https://', 'rtsp://', 'rtmp://'))
    
    def _open_video_with_retry(self, job: IngestionJob) -> bool:
        """
        Open video with retry and stream support
        Supports: Local files, HLS streams, RTSP, HTTP
        """
        is_stream = self._is_stream_url(job.config.video_path)
        job.is_live_stream = is_stream
        
        for attempt in range(job.max_retries):
            try:
                if is_stream:
                    self.logger.info(f"Opening stream URL: {job.config.video_path[:60]}...")
                    
                    # For HLS/HTTP streams, use CAP_FFMPEG backend
                    job.cap = cv2.VideoCapture(
                        job.config.video_path,
                        cv2.CAP_FFMPEG
                    )
                    
                    # Set buffer size for live streams
                    job.cap.set(cv2.CAP_PROP_BUFFERSIZE, 3)
                    
                    # Set timeout for network streams
                    job.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 10000)
                    job.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 10000)
                    
                else:
                    # Local file
                    self.logger.info(f"Opening local file: {job.config.video_path}")
                    job.cap = cv2.VideoCapture(job.config.video_path)
                
                if job.cap.isOpened():
                    # Test read a frame
                    ret, frame = job.cap.read()
                    if ret and frame is not None:
                        self.logger.info(f"✅ Video source opened successfully")
                        self.logger.info(f"   Type: {'Live stream' if is_stream else 'Local file'}")
                        self.logger.info(f"   Frame size: {frame.shape[1]}x{frame.shape[0]}")
                        
                        # Reset position for local files
                        if not is_stream:
                            job.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        
                        return True
                    else:
                        raise Exception("Cannot read frames from source")
                else:
                    raise Exception("Failed to open video source")
                    
            except Exception as e:
                if attempt < job.max_retries - 1:
                    wait_time = job.backoff_time * (2 ** attempt)
                    self.logger.warning(
                        f"Video open attempt {attempt+1} failed, "
                        f"retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to open video after {job.max_retries} attempts")
                    return False
        
        return False
    
    def _reconnect_stream(self, job: IngestionJob) -> bool:
        """Reconnect to stream if disconnected"""
        if not job.is_live_stream:
            return False
        
        if job.stream_reconnect_count >= job.max_reconnect_attempts:
            self.logger.error(f"Max reconnect attempts ({job.max_reconnect_attempts}) reached")
            return False
        
        job.stream_reconnect_count += 1
        self.logger.warning(f"Stream disconnected, attempting reconnect {job.stream_reconnect_count}...")
        
        # Release old capture
        if job.cap:
            job.cap.release()
        
        # Wait before reconnect
        time.sleep(2 * job.stream_reconnect_count)
        
        # Try to reopen
        return self._open_video_with_retry(job)
    
    def _calculate_backpressure(self, job: IngestionJob) -> BackpressureMetrics:
        """Calculate backpressure and suggest adjustments"""
        queue_len = job.queue.qsize()
        
        # Calculate average processing time
        avg_time = sum(job.processing_times) / len(job.processing_times) if job.processing_times else 0
        
        # Determine pressure level
        if queue_len < 20:
            pressure_level = "normal"
            suggested_fps = min(job.current_fps + 5, job.config.max_fps)
            suggested_batch = min(job.current_batch_size + 2, 20)
        elif queue_len < 40:
            pressure_level = "moderate"
            suggested_fps = job.current_fps
            suggested_batch = job.current_batch_size
        elif queue_len < 70:
            pressure_level = "high"
            suggested_fps = max(job.current_fps - 5, job.config.min_fps)
            suggested_batch = max(job.current_batch_size - 2, 5)
        else:
            pressure_level = "critical"
            suggested_fps = job.config.min_fps
            suggested_batch = 5
        
        return BackpressureMetrics(
            queue_length=queue_len,
            avg_processing_time=avg_time,
            pressure_level=pressure_level,
            suggested_fps=suggested_fps,
            suggested_batch_size=suggested_batch
        )
    
    async def _apply_backpressure_adjustment(self, job: IngestionJob):
        """Phase 3: Automatic backpressure adjustment"""
        if not job.config.adaptive_mode:
            return
        
        # Check cooldown
        now = time.time()
        if now - job.last_adjustment_time < job.adjustment_cooldown:
            return
        
        metrics = self._calculate_backpressure(job)
        
        # Apply adjustments
        if metrics.pressure_level in ["high", "critical"]:
            old_fps = job.current_fps
            job.current_fps = metrics.suggested_fps
            job.current_batch_size = metrics.suggested_batch_size
            
            self.logger.warning(
                f"🔻 Backpressure detected ({metrics.pressure_level}): "
                f"Reduced FPS {old_fps}→{job.current_fps}, "
                f"batch {job.current_batch_size}"
            )
            
            job.last_adjustment_time = now
            
        elif metrics.pressure_level == "normal" and job.current_fps < job.config.max_fps:
            old_fps = job.current_fps
            job.current_fps = min(metrics.suggested_fps, job.config.max_fps)
            
            self.logger.info(
                f"🔺 Queue normal: Increased FPS {old_fps}→{job.current_fps}"
            )
            
            job.last_adjustment_time = now
    
    async def _run_ingestion(self, job: IngestionJob):
        """Main ingestion loop with self-healing and stream support"""
        try:
            job.status = "running"
            
            # Get processing agent
            if not self.processing_agent_url:
                await self._discover_processing_agent()
            
            # Open video with retry
            if not self._open_video_with_retry(job):
                raise Exception("Failed to open video source")
            
            # Get video properties
            video_fps = job.cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(job.cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            # Handle live streams (FPS might be 0 or unknown)
            if video_fps == 0 or video_fps > 1000:
                video_fps = 25  # Default FPS for live streams
                self.logger.warning(f"FPS not available, using default: {video_fps}")
            
            if total_frames <= 0 or job.is_live_stream:
                total_frames = float('inf')  # Infinite for live streams
                self.logger.info(f"Live stream mode: infinite duration")
            else:
                self.logger.info(f"Video: {video_fps} FPS, {total_frames} frames")
            
            # Set start frame for local files
            if not job.is_live_stream and job.config.start_frame > 0:
                job.cap.set(cv2.CAP_PROP_POS_FRAMES, job.config.start_frame)
            
            batch = []
            frame_count = job.config.start_frame
            consecutive_read_failures = 0
            max_consecutive_failures = 10
            
            while not job.stop_flag.is_set():
                # Check pause
                if job.pause_flag.is_set():
                    await asyncio.sleep(0.5)
                    continue
                
                # Apply backpressure adjustment
                await self._apply_backpressure_adjustment(job)
                
                # Check end frame (for local files)
                if not job.is_live_stream and job.config.end_frame:
                    if frame_count >= job.config.end_frame:
                        break
                
                # Read frame
                ret, frame = job.cap.read()
                
                if not ret or frame is None:
                    consecutive_read_failures += 1
                    
                    if consecutive_read_failures >= max_consecutive_failures:
                        if job.is_live_stream:
                            # Try to reconnect for streams
                            self.logger.warning(f"Stream read failures, attempting reconnect...")
                            if self._reconnect_stream(job):
                                consecutive_read_failures = 0
                                continue
                            else:
                                self.logger.error("Failed to reconnect to stream")
                                break
                        else:
                            # End of local file
                            self.logger.info("End of video file reached")
                            break
                    
                    # Wait a bit before retry
                    await asyncio.sleep(0.1)
                    continue
                
                # Reset failure counter on successful read
                consecutive_read_failures = 0
                frame_count += 1
                
                # Calculate frame skip based on current FPS
                frame_skip = max(1, int(video_fps / job.current_fps))
                
                if (frame_count - job.config.start_frame) % frame_skip != 0:
                    job.frames_dropped += 1
                    continue
                
                # Quality check: drop bad frames
                if not self._is_frame_quality_good(frame):
                    job.frames_dropped += 1
                    continue
                
                # Resize if needed
                if job.config.resolution:
                    frame = cv2.resize(
                        frame,
                        (job.config.resolution['width'], job.config.resolution['height'])
                    )
                
                # Encode frame
                _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                frame_b64 = base64.b64encode(buffer).decode('utf-8')
                
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
                if len(batch) >= job.current_batch_size:
                    start_time = time.time()
                    success = await self._send_batch_with_retry(job, batch)
                    
                    if success:
                        processing_time = time.time() - start_time
                        job.processing_times.append(processing_time)
                        batch = []
                        job.retry_count = 0  # Reset on success
                    else:
                        # Retry logic will handle this
                        pass
                
                # Rate limiting
                await asyncio.sleep(1.0 / job.current_fps)
            
            # Send remaining
            if batch:
                await self._send_batch_with_retry(job, batch)
            
            job.status = "completed"
            self.logger.info(
                f"Job {job.job_id} completed: "
                f"{job.frames_ingested} frames ingested, "
                f"{job.frames_dropped} dropped"
            )
            
        except Exception as e:
            job.status = "failed"
            self.logger.error(f"Ingestion job failed: {e}")
            raise
        finally:
            if job.cap:
                job.cap.release()
    
    def _is_frame_quality_good(self, frame) -> bool:
        """Phase 3: Frame quality check"""
        # Check if frame is too dark or blurry
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # Brightness check
        mean_brightness = gray.mean()
        if mean_brightness < 20 or mean_brightness > 250:
            return False
        
        # Blur check (Laplacian variance)
        laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
        if laplacian_var < 50:  # Too blurry
            return False
        
        return True
    
    async def _send_batch_with_retry(self, job: IngestionJob, batch: list) -> bool:
        """Phase 3: Send batch with exponential backoff retry"""
        for attempt in range(job.max_retries):
            try:
                batch_id = str(uuid.uuid4())
                
                payload = {
                    "batch_id": batch_id,
                    "source_id": job.config.source_id,
                    "frames": batch
                }
                
                await self.send_message(
                    receiver="processing",
                    message_type="process_batch",
                    payload=payload,
                    receiver_url=self.processing_agent_url
                )
                
                self.logger.debug(f"Sent batch {batch_id} with {len(batch)} frames")
                return True
                
            except Exception as e:
                job.retry_count += 1
                
                if attempt < job.max_retries - 1:
                    wait_time = job.backoff_time * (2 ** attempt)
                    self.logger.warning(
                        f"Batch send attempt {attempt+1} failed, "
                        f"retrying in {wait_time}s: {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to send batch after {job.max_retries} attempts")
                    return False
        
        return False
    
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
            
            for agent in agents:
                if agent['agent_type'] == 'processing':
                    self.processing_agent_url = "http://processing:8002"
                    self.logger.info(f"Found processing agent: {self.processing_agent_url}")
                    return
            
            self.logger.warning("No processing agent found")
            
        except Exception as e:
            self.logger.error(f"Failed to discover processing agent: {e}")
    
    async def get_capabilities(self) -> list[str]:
        """Return Phase 3 capabilities with stream support"""
        return [
            "video_ingestion",
            "frame_capture",
            "batch_processing",
            "adaptive_fps",
            "backpressure_handling",
            "self_healing",
            "quality_check",
            "auto_retry",
            "hls_stream_support",
            "rtsp_stream_support",
            "http_stream_support",
            "stream_reconnection"
        ]
    
    async def on_startup(self):
        self.logger.info("Phase 3 Ingestion Agent started with stream support")
        if self.orchestrator_url:
            await asyncio.sleep(2)
            await self._discover_processing_agent()
    
    async def on_shutdown(self):
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