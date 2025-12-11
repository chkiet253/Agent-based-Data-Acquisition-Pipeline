"""
Processing Agent - Phase 2 Implementation
Performs object detection on video frames
"""
import asyncio
import base64
import cv2
import numpy as np
import os
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

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


class ProcessingAgent(BaseAgent):
    """
    Processing Agent - Detects objects in video frames
    Uses YOLO or Haar Cascade for detection
    """
    
    def __init__(self, agent_type: str = "processing", port: int = 8002, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        # Processing queue
        self.processing_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.processing_task: Optional[asyncio.Task] = None
        
        # Detector (using Haar Cascade for simplicity - can be replaced with YOLO)
        self.detector = None
        self.detector_type = "haar_cascade"  # or "yolo"
        
        # Storage agent info
        self.storage_agent_url: Optional[str] = None
        
        # Metrics
        self.processed_batches = 0
        self.processed_frames = 0
        self.detection_count = 0
    
    def setup_custom_routes(self):
        """Setup processing-specific routes"""
        
        @self.app.post("/process/batch")
        async def process_batch(batch: ProcessingBatch):
            """Process a batch of frames"""
            try:
                # Add to queue
                await self.processing_queue.put(batch)
                
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
            """Get processing status"""
            return {
                "queue_length": self.processing_queue.qsize(),
                "processed_batches": self.processed_batches,
                "processed_frames": self.processed_frames,
                "detection_count": self.detection_count,
                "status": self.status
            }
        
        @self.app.get("/process/metrics")
        async def get_metrics():
            """Get detailed metrics"""
            return await self.get_metrics()
    
    async def handle_message(self, message: TaskMessage) -> Dict[str, Any]:
        """Handle incoming messages"""
        if message.message_type == "process_batch":
            # Convert payload to ProcessingBatch
            try:
                batch = ProcessingBatch(**message.payload)
                await self.processing_queue.put(batch)
                
                return {
                    "status": "queued",
                    "batch_id": batch.batch_id,
                    "queue_length": self.processing_queue.qsize()
                }
            except Exception as e:
                self.logger.error(f"Failed to handle batch message: {e}")
                return {"status": "error", "error": str(e)}
        
        return await super().handle_message(message)
    
    def _load_detector(self):
        """Load object detector"""
        try:
            if self.detector_type == "haar_cascade":
                # Load Haar Cascade for vehicle detection (or face for testing)
                cascade_path = cv2.data.haarcascades + 'haarcascade_car.xml'
                
                # Fallback to frontalface if car cascade not available
                if not os.path.exists(cascade_path):
                    cascade_path = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
                    self.logger.warning("Car cascade not found, using face cascade for testing")
                
                self.detector = cv2.CascadeClassifier(cascade_path)
                self.logger.info(f"Loaded Haar Cascade detector: {cascade_path}")
            
            # TODO: Add YOLO support
            # elif self.detector_type == "yolo":
            #     self.detector = cv2.dnn.readNet("yolov4-tiny.weights", "yolov4-tiny.cfg")
            
        except Exception as e:
            self.logger.error(f"Failed to load detector: {e}")
            self.detector = None
    
    def _detect_objects(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        """Detect objects in frame"""
        if self.detector is None:
            return []
        
        try:
            # Convert to grayscale for Haar Cascade
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            # Detect objects
            detections = self.detector.detectMultiScale(
                gray,
                scaleFactor=1.1,
                minNeighbors=5,
                minSize=(30, 30)
            )
            
            # Convert to list of dicts
            results = []
            for (x, y, w, h) in detections:
                results.append({
                    "bbox": [int(x), int(y), int(w), int(h)],
                    "confidence": 0.9,  # Haar doesn't provide confidence
                    "class": "object",
                    "class_id": 0
                })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Detection failed: {e}")
            return []
    
    def _anonymize_frame(self, frame: np.ndarray, detections: List[Dict[str, Any]]) -> np.ndarray:
        """Anonymize detected objects by blurring them"""
        anonymized = frame.copy()
        
        for det in detections:
            x, y, w, h = det['bbox']
            
            # Extract ROI
            roi = anonymized[y:y+h, x:x+w]
            
            # Apply strong blur
            blurred = cv2.GaussianBlur(roi, (99, 99), 30)
            
            # Put blurred region back
            anonymized[y:y+h, x:x+w] = blurred
        
        return anonymized
    
    async def _process_batch_worker(self):
        """Worker that processes batches from queue"""
        while True:
            try:
                # Get batch from queue
                batch = await self.processing_queue.get()
                
                self.logger.info(f"Processing batch {batch.batch_id} with {len(batch.frames)} frames")
                
                results = []
                
                for frame_data in batch.frames:
                    # Decode frame
                    frame_b64 = frame_data['data']
                    frame_bytes = base64.b64decode(frame_b64)
                    frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                    
                    if frame is None:
                        self.logger.error(f"Failed to decode frame {frame_data['frame_id']}")
                        continue
                    
                    # Detect objects
                    detections = self._detect_objects(frame)
                    self.detection_count += len(detections)
                    
                    # Anonymize if enabled
                    anonymized = False
                    if batch.config and batch.config.get('enable_anonymization', False):
                        frame = self._anonymize_frame(frame, detections)
                        anonymized = True
                    
                    # Encode processed frame
                    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                    processed_b64 = base64.b64encode(buffer).decode('utf-8')
                    
                    # Build result
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
                
                # Send to storage
                await self._send_to_storage(batch.batch_id, batch.source_id, results)
                
                self.processed_batches += 1
                self.processing_queue.task_done()
                
                self.logger.info(f"Completed batch {batch.batch_id}: {len(results)} frames, {sum(r['detection_count'] for r in results)} detections")
                
            except Exception as e:
                self.logger.error(f"Batch processing error: {e}")
    
    async def _send_to_storage(self, batch_id: str, source_id: str, results: List[Dict]):
        """Send processed results to storage agent"""
        if not self.storage_agent_url:
            await self._discover_storage_agent()
        
        if not self.storage_agent_url:
            self.logger.warning("No storage agent available")
            return
        
        try:
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
            
        except Exception as e:
            self.logger.error(f"Failed to send to storage: {e}")
    
    async def _discover_storage_agent(self):
        """Discover storage agent from orchestrator"""
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
                    self.logger.info(f"Found storage agent: {self.storage_agent_url}")
                    return
            
            self.logger.warning("No storage agent found")
            
        except Exception as e:
            self.logger.error(f"Failed to discover storage agent: {e}")
    
    async def get_capabilities(self) -> list[str]:
        return ["object_detection", "frame_processing", "anonymization"]
    
    async def get_metrics(self) -> Dict[str, Any]:
        base_metrics = await super().get_metrics()
        base_metrics.update({
            "queue_length": self.processing_queue.qsize(),
            "processed_batches": self.processed_batches,
            "processed_frames": self.processed_frames,
            "detection_count": self.detection_count
        })
        return base_metrics
    
    async def on_startup(self):
        self.logger.info("Processing Agent started")
        
        # Load detector
        self._load_detector()
        
        # Start processing worker
        self.processing_task = asyncio.create_task(self._process_batch_worker())
        
        # Discover storage agent
        if self.orchestrator_url:
            await asyncio.sleep(2)
            await self._discover_storage_agent()
    
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