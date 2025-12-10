"""
Storage Agent - Phase 2 Implementation
Stores processed results to Parquet and MinIO
"""
import asyncio
import io
import json
import os
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
from minio import Minio
from minio.error import S3Error

from fastapi import HTTPException
from pydantic import BaseModel

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from base.base_agent import BaseAgent, TaskMessage


# Models
class StorageRequest(BaseModel):
    batch_id: str
    source_id: str
    results: List[Dict[str, Any]]
    timestamp: str


class StorageAgent(BaseAgent):
    """
    Storage Agent - Stores processed data to Parquet and MinIO
    """
    
    def __init__(self, agent_type: str = "storage", port: int = 8003, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        # MinIO client
        self.minio_client: Optional[Minio] = None
        self.bucket_name = os.getenv("MINIO_BUCKET", "pipeline-data")
        
        # Storage queue
        self.storage_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.storage_task: Optional[asyncio.Task] = None
        
        # Metrics
        self.batches_stored = 0
        self.frames_stored = 0
        self.total_bytes_stored = 0
    
    def setup_custom_routes(self):
        """Setup storage-specific routes"""
        
        @self.app.post("/storage/write")
        async def write_data(request: StorageRequest):
            """Write data to storage"""
            try:
                await self.storage_queue.put(request)
                
                return {
                    "batch_id": request.batch_id,
                    "status": "queued",
                    "queue_length": self.storage_queue.qsize()
                }
                
            except Exception as e:
                self.logger.error(f"Failed to queue storage request: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/storage/status")
        async def get_status():
            """Get storage status"""
            return {
                "queue_length": self.storage_queue.qsize(),
                "batches_stored": self.batches_stored,
                "frames_stored": self.frames_stored,
                "total_bytes_stored": self.total_bytes_stored,
                "minio_connected": self.minio_client is not None,
                "status": self.status
            }
        
        @self.app.get("/storage/buckets")
        async def list_buckets():
            """List MinIO buckets"""
            if not self.minio_client:
                raise HTTPException(status_code=503, detail="MinIO not connected")
            
            try:
                buckets = self.minio_client.list_buckets()
                return {
                    "buckets": [{"name": b.name, "creation_date": str(b.creation_date)} for b in buckets]
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/storage/objects/{source_id}")
        async def list_objects(source_id: str):
            """List objects for a source"""
            if not self.minio_client:
                raise HTTPException(status_code=503, detail="MinIO not connected")
            
            try:
                objects = self.minio_client.list_objects(
                    self.bucket_name,
                    prefix=f"{source_id}/",
                    recursive=True
                )
                
                return {
                    "source_id": source_id,
                    "objects": [
                        {
                            "name": obj.object_name,
                            "size": obj.size,
                            "last_modified": str(obj.last_modified)
                        }
                        for obj in objects
                    ]
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
    
    async def handle_message(self, message: TaskMessage) -> Dict[str, Any]:
        """Handle incoming messages"""
        if message.message_type == "store_results":
            try:
                request = StorageRequest(**message.payload)
                await self.storage_queue.put(request)
                
                return {
                    "status": "queued",
                    "batch_id": request.batch_id
                }
            except Exception as e:
                self.logger.error(f"Failed to handle store message: {e}")
                return {"status": "error", "error": str(e)}
        
        return await super().handle_message(message)
    
    def _init_minio(self):
        """Initialize MinIO client"""
        try:
            endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
            
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False
            )
            
            # Create bucket if not exists
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                self.logger.info(f"Created bucket: {self.bucket_name}")
            else:
                self.logger.info(f"Using existing bucket: {self.bucket_name}")
            
            self.logger.info(f"MinIO connected: {endpoint}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize MinIO: {e}")
            self.minio_client = None
    
    def _create_parquet_from_results(self, results: List[Dict]) -> bytes:
        """Convert results to Parquet format"""
        # Flatten results for DataFrame
        rows = []
        for result in results:
            base_row = {
                "frame_id": result['frame_id'],
                "sequence_number": result['sequence_number'],
                "timestamp": result['timestamp'],
                "detection_count": result['detection_count'],
                "anonymized": result['anonymized'],
                "width": result['metadata']['width'],
                "height": result['metadata']['height'],
                "format": result['metadata']['format'],
                "size_bytes": result['metadata']['size_bytes']
            }
            
            # Add detections as JSON string
            base_row['detections'] = json.dumps(result['detections'])
            
            rows.append(base_row)
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Convert to Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy')
        buffer.seek(0)
        
        return buffer.getvalue()
    
    def _create_metadata_json(self, batch_id: str, source_id: str, results: List[Dict]) -> bytes:
        """Create metadata JSON"""
        metadata = {
            "batch_id": batch_id,
            "source_id": source_id,
            "frame_count": len(results),
            "total_detections": sum(r['detection_count'] for r in results),
            "timestamp": datetime.utcnow().isoformat(),
            "time_range": {
                "start": min(r['timestamp'] for r in results),
                "end": max(r['timestamp'] for r in results)
            },
            "frames": [
                {
                    "frame_id": r['frame_id'],
                    "sequence_number": r['sequence_number'],
                    "detection_count": r['detection_count']
                }
                for r in results
            ]
        }
        
        return json.dumps(metadata, indent=2).encode('utf-8')
    
    async def _storage_worker(self):
        """Worker that processes storage requests"""
        while True:
            try:
                request = await self.storage_queue.get()
                
                self.logger.info(f"Storing batch {request.batch_id}: {len(request.results)} frames")
                
                if not self.minio_client:
                    self.logger.error("MinIO not available")
                    continue
                
                # Create Parquet file
                parquet_data = self._create_parquet_from_results(request.results)
                parquet_size = len(parquet_data)
                
                # Create metadata
                metadata_data = self._create_metadata_json(
                    request.batch_id,
                    request.source_id,
                    request.results
                )
                
                # Upload to MinIO
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                
                # Upload Parquet
                parquet_path = f"{request.source_id}/data/{timestamp}_{request.batch_id}.parquet"
                self.minio_client.put_object(
                    self.bucket_name,
                    parquet_path,
                    io.BytesIO(parquet_data),
                    length=parquet_size,
                    content_type="application/octet-stream"
                )
                
                # Upload metadata
                metadata_path = f"{request.source_id}/metadata/{timestamp}_{request.batch_id}.json"
                self.minio_client.put_object(
                    self.bucket_name,
                    metadata_path,
                    io.BytesIO(metadata_data),
                    length=len(metadata_data),
                    content_type="application/json"
                )
                
                # Update metrics
                self.batches_stored += 1
                self.frames_stored += len(request.results)
                self.total_bytes_stored += parquet_size + len(metadata_data)
                
                self.logger.info(
                    f"Stored batch {request.batch_id}: "
                    f"{parquet_size} bytes Parquet + {len(metadata_data)} bytes metadata"
                )
                
                self.storage_queue.task_done()
                
            except S3Error as e:
                self.logger.error(f"MinIO error: {e}")
            except Exception as e:
                self.logger.error(f"Storage worker error: {e}")
    
    async def get_capabilities(self) -> list[str]:
        return ["parquet_storage", "metadata_storage", "minio_integration"]
    
    async def get_metrics(self) -> Dict[str, Any]:
        base_metrics = await super().get_metrics()
        base_metrics.update({
            "queue_length": self.storage_queue.qsize(),
            "batches_stored": self.batches_stored,
            "frames_stored": self.frames_stored,
            "total_bytes_stored": self.total_bytes_stored,
            "minio_connected": self.minio_client is not None
        })
        return base_metrics
    
    async def on_startup(self):
        self.logger.info("Storage Agent started")
        
        # Initialize MinIO
        self._init_minio()
        
        # Start storage worker
        self.storage_task = asyncio.create_task(self._storage_worker())
    
    async def on_shutdown(self):
        if self.storage_task:
            self.storage_task.cancel()
        
        self.logger.info("Storage Agent stopped")


if __name__ == "__main__":
    agent = StorageAgent(
        port=8003,
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000")
    )
    agent.run()