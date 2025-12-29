"""
Phase 3: Storage Agent with Autonomy Features
- Automatic fallback to local storage
- MinIO health monitoring
- Automatic cleanup when disk full
- Self-healing reconnection
"""
import asyncio
import io
import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from enum import Enum

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


class StorageMode(str, Enum):
    PRIMARY = "primary"  # MinIO
    FALLBACK = "fallback"  # Local disk


class StorageAgent(BaseAgent):
    """
    Phase 3: Autonomous Storage Agent
    - Auto fallback when MinIO fails
    - Health monitoring
    - Automatic cleanup
    """
    
    def __init__(self, agent_type: str = "storage", port: int = 8003, **kwargs):
        super().__init__(agent_type=agent_type, port=port, **kwargs)
        
        self.minio_client: Optional[Minio] = None
        self.bucket_name = os.getenv("MINIO_BUCKET", "pipeline-data")
        
        # Phase 3: Storage mode
        self.storage_mode = StorageMode.PRIMARY
        self.fallback_path = Path("/data/fallback_storage")
        self.fallback_path.mkdir(parents=True, exist_ok=True)
        
        # Health monitoring
        self.health_check_interval = 30
        self.health_check_task: Optional[asyncio.Task] = None
        self.minio_healthy = False
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        
        # Storage queue
        self.storage_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.storage_task: Optional[asyncio.Task] = None
        
        # Cleanup thresholds
        self.disk_threshold_percent = 85  # Trigger cleanup at 85%
        self.cleanup_target_percent = 70  # Clean until 70%
        
        # Metrics
        self.batches_stored = 0
        self.frames_stored = 0
        self.total_bytes_stored = 0
        self.fallback_count = 0
        self.cleanup_count = 0
    
    def setup_custom_routes(self):
        """Setup storage-specific routes"""
        
        @self.app.post("/storage/write")
        async def write_data(request: StorageRequest):
            try:
                await self.storage_queue.put(request)
                
                return {
                    "batch_id": request.batch_id,
                    "status": "queued",
                    "queue_length": self.storage_queue.qsize(),
                    "storage_mode": self.storage_mode.value
                }
                
            except Exception as e:
                self.logger.error(f"Failed to queue storage request: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/storage/status")
        async def get_status():
            disk_usage = self._get_disk_usage()
            
            return {
                "queue_length": self.storage_queue.qsize(),
                "batches_stored": self.batches_stored,
                "frames_stored": self.frames_stored,
                "total_bytes_stored": self.total_bytes_stored,
                "storage_mode": self.storage_mode.value,
                "minio_healthy": self.minio_healthy,
                "disk_usage_percent": disk_usage,
                "fallback_count": self.fallback_count,
                "cleanup_count": self.cleanup_count,
                "status": self.status
            }
        
        @self.app.get("/storage/health")
        async def health_check():
            """Detailed health check"""
            disk_usage = self._get_disk_usage()
            
            health_status = "healthy"
            if disk_usage > self.disk_threshold_percent:
                health_status = "degraded"
            if not self.minio_healthy and self.storage_mode == StorageMode.FALLBACK:
                health_status = "degraded"
            
            return {
                "status": health_status,
                "storage_mode": self.storage_mode.value,
                "minio_healthy": self.minio_healthy,
                "disk_usage_percent": disk_usage,
                "consecutive_failures": self.consecutive_failures
            }
        
        @self.app.post("/storage/mode/switch")
        async def switch_mode(mode: str):
            """Manual mode switch"""
            try:
                new_mode = StorageMode(mode)
                old_mode = self.storage_mode
                self.storage_mode = new_mode
                
                self.logger.info(f"Storage mode switched: {old_mode.value} → {new_mode.value}")
                
                return {
                    "old_mode": old_mode.value,
                    "new_mode": new_mode.value,
                    "status": "switched"
                }
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}")
        
        @self.app.post("/storage/cleanup")
        async def manual_cleanup():
            """Manual cleanup trigger"""
            freed_bytes = await self._cleanup_old_data()
            
            return {
                "status": "completed",
                "freed_bytes": freed_bytes,
                "disk_usage_percent": self._get_disk_usage()
            }
        
        @self.app.get("/storage/buckets")
        async def list_buckets():
            if not self.minio_client or not self.minio_healthy:
                raise HTTPException(status_code=503, detail="MinIO not available")
            
            try:
                buckets = self.minio_client.list_buckets()
                return {
                    "buckets": [
                        {"name": b.name, "creation_date": str(b.creation_date)} 
                        for b in buckets
                    ]
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/storage/objects/{source_id}")
        async def list_objects(source_id: str):
            if self.storage_mode == StorageMode.FALLBACK:
                # List from fallback storage
                source_path = self.fallback_path / source_id
                if not source_path.exists():
                    return {"source_id": source_id, "objects": []}
                
                objects = []
                for file_path in source_path.rglob("*"):
                    if file_path.is_file():
                        stat = file_path.stat()
                        objects.append({
                            "name": str(file_path.relative_to(self.fallback_path)),
                            "size": stat.st_size,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
                
                return {"source_id": source_id, "objects": objects, "storage": "fallback"}
            
            else:
                # List from MinIO
                if not self.minio_client or not self.minio_healthy:
                    raise HTTPException(status_code=503, detail="MinIO not available")
                
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
                        ],
                        "storage": "minio"
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
                    "batch_id": request.batch_id,
                    "storage_mode": self.storage_mode.value
                }
            except Exception as e:
                self.logger.error(f"Failed to handle store message: {e}")
                return {"status": "error", "error": str(e)}
        
        return await super().handle_message(message)
    
    def _init_minio(self):
        """Initialize MinIO client"""
        try:
            endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
            
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False
            )
            
            # Test connection
            self.minio_client.list_buckets()
            
            # Create bucket if not exists
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                self.logger.info(f"Created bucket: {self.bucket_name}")
            else:
                self.logger.info(f"Using existing bucket: {self.bucket_name}")
            
            self.minio_healthy = True
            self.consecutive_failures = 0
            self.logger.info(f"✅ MinIO connected: {endpoint}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize MinIO: {e}")
            self.minio_client = None
            self.minio_healthy = False
    
    async def _health_check_loop(self):
        """Phase 3: Periodic MinIO health check"""
        while True:
            await asyncio.sleep(self.health_check_interval)
            
            try:
                if self.minio_client:
                    # Test MinIO with simple operation
                    self.minio_client.list_buckets()
                    
                    if not self.minio_healthy:
                        self.logger.info("✅ MinIO recovered!")
                        self.minio_healthy = True
                        self.consecutive_failures = 0
                        
                        # Switch back to PRIMARY mode
                        if self.storage_mode == StorageMode.FALLBACK:
                            self.storage_mode = StorageMode.PRIMARY
                            self.logger.info("🔄 Switched back to PRIMARY storage mode")
                
            except Exception as e:
                self.consecutive_failures += 1
                
                if self.minio_healthy:
                    self.logger.warning(f"⚠️ MinIO health check failed: {e}")
                
                if self.consecutive_failures >= self.max_consecutive_failures:
                    self.minio_healthy = False
                    
                    if self.storage_mode == StorageMode.PRIMARY:
                        self.storage_mode = StorageMode.FALLBACK
                        self.logger.error(
                            f"❌ MinIO unhealthy after {self.consecutive_failures} checks. "
                            f"Switched to FALLBACK mode"
                        )
    
    def _get_disk_usage(self) -> float:
        """Get disk usage percentage"""
        try:
            stat = shutil.disk_usage(self.fallback_path)
            return (stat.used / stat.total) * 100
        except Exception as e:
            self.logger.error(f"Failed to get disk usage: {e}")
            return 0.0
    
    async def _cleanup_old_data(self) -> int:
        """Phase 3: Cleanup old data when disk is full"""
        try:
            disk_usage = self._get_disk_usage()
            
            if disk_usage < self.disk_threshold_percent:
                return 0
            
            self.logger.warning(
                f"🧹 Disk usage {disk_usage:.1f}% exceeds threshold. Starting cleanup..."
            )
            
            # Find oldest files in fallback storage
            files = []
            for file_path in self.fallback_path.rglob("*"):
                if file_path.is_file():
                    stat = file_path.stat()
                    files.append((file_path, stat.st_mtime, stat.st_size))
            
            # Sort by modification time (oldest first)
            files.sort(key=lambda x: x[1])
            
            freed_bytes = 0
            target_usage = self.cleanup_target_percent
            
            for file_path, mtime, size in files:
                if self._get_disk_usage() <= target_usage:
                    break
                
                try:
                    file_path.unlink()
                    freed_bytes += size
                    self.logger.info(f"Deleted old file: {file_path.name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete {file_path}: {e}")
            
            self.cleanup_count += 1
            
            self.logger.info(
                f"✅ Cleanup completed: freed {freed_bytes:,} bytes, "
                f"disk usage now {self._get_disk_usage():.1f}%"
            )
            
            return freed_bytes
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
            return 0
    
    def _create_parquet_from_results(self, results: List[Dict]) -> bytes:
        """Convert results to Parquet format"""
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
            
            base_row['detections'] = json.dumps(result['detections'])
            
            rows.append(base_row)
        
        df = pd.DataFrame(rows)
        
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
            "storage_mode": self.storage_mode.value,
            "time_range": {
                "start": min(r['timestamp'] for r in results),
                "end": max(r['timestamp'] for r in results)
            }
        }
        
        return json.dumps(metadata, indent=2).encode('utf-8')
    
    async def _storage_worker(self):
        """Worker that processes storage requests"""
        while True:
            try:
                request = await self.storage_queue.get()
                
                self.logger.info(
                    f"Storing batch {request.batch_id}: {len(request.results)} frames "
                    f"[mode: {self.storage_mode.value}]"
                )
                
                # Check disk usage and cleanup if needed
                if self._get_disk_usage() > self.disk_threshold_percent:
                    await self._cleanup_old_data()
                
                # Create data
                parquet_data = self._create_parquet_from_results(request.results)
                metadata_data = self._create_metadata_json(
                    request.batch_id,
                    request.source_id,
                    request.results
                )
                
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                
                # Store based on mode
                if self.storage_mode == StorageMode.PRIMARY and self.minio_healthy:
                    try:
                        await self._store_to_minio(
                            request.source_id,
                            timestamp,
                            request.batch_id,
                            parquet_data,
                            metadata_data
                        )
                    except Exception as e:
                        self.logger.error(f"MinIO storage failed, falling back: {e}")
                        self.fallback_count += 1
                        await self._store_to_fallback(
                            request.source_id,
                            timestamp,
                            request.batch_id,
                            parquet_data,
                            metadata_data
                        )
                else:
                    # Use fallback storage
                    self.fallback_count += 1
                    await self._store_to_fallback(
                        request.source_id,
                        timestamp,
                        request.batch_id,
                        parquet_data,
                        metadata_data
                    )
                
                # Update metrics
                self.batches_stored += 1
                self.frames_stored += len(request.results)
                self.total_bytes_stored += len(parquet_data) + len(metadata_data)
                
                self.storage_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Storage worker error: {e}")
    
    async def _store_to_minio(
        self,
        source_id: str,
        timestamp: str,
        batch_id: str,
        parquet_data: bytes,
        metadata_data: bytes
    ):
        """Store to MinIO"""
        parquet_path = f"{source_id}/data/{timestamp}_{batch_id}.parquet"
        self.minio_client.put_object(
            self.bucket_name,
            parquet_path,
            io.BytesIO(parquet_data),
            length=len(parquet_data),
            content_type="application/octet-stream"
        )
        
        metadata_path = f"{source_id}/metadata/{timestamp}_{batch_id}.json"
        self.minio_client.put_object(
            self.bucket_name,
            metadata_path,
            io.BytesIO(metadata_data),
            length=len(metadata_data),
            content_type="application/json"
        )
        
        self.logger.info(f"✅ Stored to MinIO: {batch_id}")
    
    async def _store_to_fallback(
        self,
        source_id: str,
        timestamp: str,
        batch_id: str,
        parquet_data: bytes,
        metadata_data: bytes
    ):
        """Store to local fallback"""
        source_path = self.fallback_path / source_id
        source_path.mkdir(parents=True, exist_ok=True)
        
        data_path = source_path / "data"
        data_path.mkdir(exist_ok=True)
        
        metadata_path = source_path / "metadata"
        metadata_path.mkdir(exist_ok=True)
        
        # Write parquet
        parquet_file = data_path / f"{timestamp}_{batch_id}.parquet"
        parquet_file.write_bytes(parquet_data)
        
        # Write metadata
        metadata_file = metadata_path / f"{timestamp}_{batch_id}.json"
        metadata_file.write_bytes(metadata_data)
        
        self.logger.info(f"💾 Stored to fallback: {batch_id}")
    
    async def get_capabilities(self) -> list[str]:
        return [
            "parquet_storage",
            "metadata_storage",
            "minio_integration",
            "fallback_storage",
            "auto_cleanup",
            "health_monitoring"
        ]
    
    async def get_metrics(self) -> Dict[str, Any]:
        base_metrics = await super().get_metrics()
        base_metrics.update({
            "queue_length": self.storage_queue.qsize(),
            "batches_stored": self.batches_stored,
            "frames_stored": self.frames_stored,
            "total_bytes_stored": self.total_bytes_stored,
            "storage_mode": self.storage_mode.value,
            "minio_healthy": self.minio_healthy,
            "disk_usage_percent": self._get_disk_usage(),
            "fallback_count": self.fallback_count,
            "cleanup_count": self.cleanup_count
        })
        return base_metrics
    
    async def on_startup(self):
        self.logger.info("Phase 3 Storage Agent started with autonomy features")
        
        # Initialize MinIO
        self._init_minio()
        
        # Start health check loop
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        # Start storage worker
        self.storage_task = asyncio.create_task(self._storage_worker())
    
    async def on_shutdown(self):
        if self.storage_task:
            self.storage_task.cancel()
        
        if self.health_check_task:
            self.health_check_task.cancel()
        
        self.logger.info("Storage Agent stopped")


if __name__ == "__main__":
    agent = StorageAgent(
        port=8003,
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000")
    )
    agent.run()