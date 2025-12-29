"""
Phase 3: Processing Agent with Autonomy Features
- Circuit breaker for storage agent
- Self-healing (retry with backoff)
- Backpressure signaling to ingestion
- Adaptive processing rate
"""
import asyncio
import base64
import cv2
import numpy as np
import os
import uuid
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from enum import Enum

from fastapi import HTTPException
from pydantic import BaseModel

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from base.base_agent import BaseAgent, TaskMessage


# Models
class ProcessingBatch(BaseModel):
    batch_id: str
    source_id: str
    frames: List[Dict[str, Any]]
    config: Optional[Dict[str, Any]] = None


class CircuitState(str, Enum):
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Phase 3: Circuit breaker pattern"""
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
    
    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == CircuitState.OPEN:
            # Check if we should try recovery
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful request"""
        self.failure_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.success_count = 0
    
    def _on_failure(self):
        """Handle failed request"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN


class ProcessingAgent(BaseAgent):
    """
    Phase 3: Autonomous Processing Agent
    - Circuit breaker for downstream dependencies
    - Self-healing with retry logic
    - Backpressure signaling
    """
    
    def __init__(self, agent_type: str = "processing", port: int = 8002, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        self.processing_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.processing_task: Optional[asyncio.Task] = None
        
        self.detector = None
        self.detector_type = "haar_cascade"
        
        self.storage_agent_url: Optional[str] = None
        self.ingestion_agent_url: Optional[str] = None
        
        # Phase 3: Circuit breaker for storage
        self.storage_circuit = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0,
            success_threshold=2
        )
        
        # Metrics
        self.processed_batches = 0
        self.processed_frames = 0
        self.detection_count = 0
        self.failed_batches = 0
        self.retry_count = 0
        
        # Backpressure thresholds
        self.queue_high_watermark = 70
        self.queue_low_watermark = 20
        self.last_backpressure_signal = 0
    
    def setup_custom_routes(self):
        """Setup processing-specific routes"""
        
        @self.app.post("/process/batch")
        async def process_batch(batch: ProcessingBatch):
            try:
                await self.processing_queue.put(batch)
                
                # Check backpressure
                await self._check_and_signal_backpressure()
                
                return {
                    "batch_id": batch.batch_id,
                    "status": "queued",
                    "queue_length": self.processing_queue.qsize()
                }
                
            except Exception as e:
                self.logger.error(f"Failed to queue batch: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/process/status")
        async def get_status():
            return {
                "queue_length": self.processing_queue.qsize(),
                "processed_batches": self.processed_batches,
                "processed_frames": self.processed_frames,
                "detection_count": self.detection_count,
                "failed_batches": self.failed_batches,
                "retry_count": self.retry_count,
                "circuit_breaker_state": self.storage_circuit.state.value,
                "status": self.status
            }
        
        @self.app.get("/process/health")
        async def health_check():
            """Detailed health check"""
            queue_len = self.processing_queue.qsize()
            
            health_status = "healthy"
            if queue_len > self.queue_high_watermark:
                health_status = "degraded"
            if self.storage_circuit.state == CircuitState.OPEN:
                health_status = "unhealthy"
            
            return {
                "status": health_status,
                "queue_length": queue_len,
                "queue_capacity": 100,
                "circuit_breaker": self.storage_circuit.state.value,
                "metrics": await self.get_metrics()
            }
        
        @self.app.post("/process/circuit/reset")
        async def reset_circuit():
            """Manual circuit breaker reset"""
            self.storage_circuit.state = CircuitState.CLOSED
            self.storage_circuit.failure_count = 0
            self.logger.info("Circuit breaker manually reset")
            return {"status": "reset", "state": CircuitState.CLOSED.value}
    
    async def handle_message(self, message: TaskMessage) -> Dict[str, Any]:
        """Handle incoming messages"""
        if message.message_type == "process_batch":
            try:
                batch = ProcessingBatch(**message.payload)
                await self.processing_queue.put(batch)
                
                await self._check_and_signal_backpressure()
                
                return {
                    "status": "queued",
                    "batch_id": batch.batch_id,
                    "queue_length": self.processing_queue.qsize()
                }
            except Exception as e:
                self.logger.error(f"Failed to handle batch message: {e}")
                return {"status": "error", "error": str(e)}
        
        return await super().handle_message(message)
    
    async def _check_and_signal_backpressure(self):
        """Phase 3: Signal backpressure to ingestion agent"""
        queue_len = self.processing_queue.qsize()
        now = time.time()
        
        # Cooldown: don't spam signals
        if now - self.last_backpressure_signal < 10:
            return
        
        if queue_len > self.queue_high_watermark:
            # Signal to reduce FPS
            await self._signal_ingestion("reduce_fps", {
                "queue_length": queue_len,
                "pressure_level": "high",
                "suggested_fps": 10
            })
            self.last_backpressure_signal = now
            
        elif queue_len < self.queue_low_watermark:
            # Signal to increase FPS
            await self._signal_ingestion("increase_fps", {
                "queue_length": queue_len,
                "pressure_level": "normal",
                "suggested_fps": 30
            })
            self.last_backpressure_signal = now
    
    async def _signal_ingestion(self, action: str, data: Dict):
        """Send backpressure signal to ingestion agent"""
        if not self.ingestion_agent_url:
            await self._discover_ingestion_agent()
        
        if not self.ingestion_agent_url:
            return
        
        try:
            await self.send_message(
                receiver="ingestion",
                message_type="backpressure_signal",
                payload={"action": action, "data": data},
                receiver_url=self.ingestion_agent_url
            )
            
            self.logger.info(f"📊 Sent backpressure signal: {action}")
            
        except Exception as e:
            self.logger.warning(f"Failed to signal ingestion: {e}")
    
    def _load_detector(self):
        """Load object detector"""
        try:
            if self.detector_type == "haar_cascade":
                cascade_path = cv2.data.haarcascades + 'haarcascade_car.xml'
                
                if not os.path.exists(cascade_path):
                    cascade_path = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
                    self.logger.warning("Using face cascade for testing")
                
                self.detector = cv2.CascadeClassifier(cascade_path)
                self.logger.info(f"Loaded detector: {cascade_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load detector: {e}")
            self.detector = None
    
    def _detect_objects(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        """Detect objects in frame"""
        if self.detector is None:
            return []
        
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            detections = self.detector.detectMultiScale(
                gray,
                scaleFactor=1.1,
                minNeighbors=5,
                minSize=(30, 30)
            )
            
            results = []
            for (x, y, w, h) in detections:
                results.append({
                    "bbox": [int(x), int(y), int(w), int(h)],
                    "confidence": 0.9,
                    "class": "object",
                    "class_id": 0
                })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Detection failed: {e}")
            return []
    
    def _anonymize_frame(self, frame: np.ndarray, detections: List[Dict[str, Any]]) -> np.ndarray:
        """Anonymize detected objects"""
        anonymized = frame.copy()
        
        for det in detections:
            x, y, w, h = det['bbox']
            roi = anonymized[y:y+h, x:x+w]
            blurred = cv2.GaussianBlur(roi, (99, 99), 30)
            anonymized[y:y+h, x:x+w] = blurred
        
        return anonymized
    
    async def _process_batch_worker(self):
        """Worker that processes batches with retry logic"""
        while True:
            try:
                batch = await self.processing_queue.get()
                
                self.logger.info(f"Processing batch {batch.batch_id} with {len(batch.frames)} frames")
                
                # Process with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        results = await self._process_batch(batch)
                        
                        # Send to storage with circuit breaker
                        await self.storage_circuit.call(
                            self._send_to_storage_with_retry,
                            batch.batch_id,
                            batch.source_id,
                            results
                        )
                        
                        self.processed_batches += 1
                        break
                        
                    except Exception as e:
                        self.retry_count += 1
                        
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            self.logger.warning(
                                f"Batch processing attempt {attempt+1} failed, "
                                f"retrying in {wait_time}s: {e}"
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            self.failed_batches += 1
                            self.logger.error(
                                f"Batch {batch.batch_id} failed after {max_retries} attempts"
                            )
                
                self.processing_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Batch processing error: {e}")
    
    async def _process_batch(self, batch: ProcessingBatch) -> List[Dict]:
        """Process batch of frames"""
        results = []
        
        for frame_data in batch.frames:
            frame_b64 = frame_data['data']
            frame_bytes = base64.b64decode(frame_b64)
            frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
            frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
            
            if frame is None:
                self.logger.error(f"Failed to decode frame {frame_data['frame_id']}")
                continue
            
            detections = self._detect_objects(frame)
            self.detection_count += len(detections)
            
            anonymized = False
            if batch.config and batch.config.get('enable_anonymization', False):
                frame = self._anonymize_frame(frame, detections)
                anonymized = True
            
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
            processed_b64 = base64.b64encode(buffer).decode('utf-8')
            
            result = {
                "frame_id": frame_data['frame_id'],
                "sequence_number": frame_data['sequence_number'],
                "timestamp": frame_data['timestamp'],
                "detections": detections,
                "detection_count": len(detections),
                "anonymized": anonymized,
                "processed_frame": processed_b64,
                "metadata": frame_data['metadata']
            }
            
            results.append(result)
            self.processed_frames += 1
        
        return results
    
    async def _send_to_storage_with_retry(
        self, 
        batch_id: str, 
        source_id: str, 
        results: List[Dict]
    ):
        """Send to storage with retry logic"""
        if not self.storage_agent_url:
            await self._discover_storage_agent()
        
        if not self.storage_agent_url:
            raise Exception("Storage agent not available")
        
        payload = {
            "batch_id": batch_id,
            "source_id": source_id,
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.send_message(
            receiver="storage",
            message_type="store_results",
            payload=payload,
            receiver_url=self.storage_agent_url
        )
        
        self.logger.info(f"Sent results for batch {batch_id} to storage")
    
    async def _discover_storage_agent(self):
        """Discover storage agent"""
        if not self.orchestrator_url:
            return
        
        try:
            response = await self.http_client.get(
                f"{self.orchestrator_url}/agents"
            )
            response.raise_for_status()
            
            agents = response.json()['agents']
            
            for agent in agents:
                if agent['agent_type'] == 'storage':
                    self.storage_agent_url = "http://storage:8003"
                    self.logger.info(f"Found storage agent")
                    return
            
            self.logger.warning("No storage agent found")
            
        except Exception as e:
            self.logger.error(f"Failed to discover storage agent: {e}")
    
    async def _discover_ingestion_agent(self):
        """Discover ingestion agent"""
        if not self.orchestrator_url:
            return
        
        try:
            response = await self.http_client.get(
                f"{self.orchestrator_url}/agents"
            )
            response.raise_for_status()
            
            agents = response.json()['agents']
            
            for agent in agents:
                if agent['agent_type'] == 'ingestion':
                    self.ingestion_agent_url = "http://ingestion:8001"
                    self.logger.info(f"Found ingestion agent")
                    return
            
        except Exception as e:
            self.logger.error(f"Failed to discover ingestion agent: {e}")
    
    async def get_capabilities(self) -> list[str]:
        return [
            "object_detection", 
            "frame_processing", 
            "anonymization",
            "circuit_breaker",
            "backpressure_signaling",
            "self_healing",
            "queue_management",
            "adaptive_processing"
        ]
    
    async def get_metrics(self) -> Dict[str, Any]:
        base_metrics = await super().get_metrics()
        base_metrics.update({
            "queue_length": self.processing_queue.qsize(),
            "processed_batches": self.processed_batches,
            "processed_frames": self.processed_frames,
            "detection_count": self.detection_count,
            "failed_batches": self.failed_batches,
            "retry_count": self.retry_count,
            "circuit_breaker_state": self.storage_circuit.state.value
        })
        return base_metrics
    
    async def on_startup(self):
        self.logger.info("Phase 3 Processing Agent started with autonomy features")
        
        self._load_detector()
        
        self.processing_task = asyncio.create_task(self._process_batch_worker())
        
        if self.orchestrator_url:
            await asyncio.sleep(2)
            await self._discover_storage_agent()
            await self._discover_ingestion_agent()
    
    async def on_shutdown(self):
        if self.processing_task:
            self.processing_task.cancel()
        
        self.logger.info("Processing Agent stopped")


if __name__ == "__main__":
    agent = ProcessingAgent(
        port=8002,
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000")
    )
    agent.run()