# workers.py - Worker Agents Implementation
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Cấu hình worker"""
    name: str
    max_load: float = 80.0
    batch_size: int = 10
    processing_delay: float = 0.5  # giây
    error_rate: float = 0.05  # 5% failure rate


class BaseWorkerAgent(ABC):
    """Base class cho tất cả worker agents"""
    
    def __init__(self, config: WorkerConfig, orchestrator_url: str = "http://localhost:8000"):
        self.config = config
        self.orchestrator_url = orchestrator_url
        self.current_load = 0.0
        self.is_running = False
        self.tasks_processed = 0
        self.tasks_failed = 0
    
    @abstractmethod
    async def process_task(self, task_data: dict) -> dict:
        """Xử lý task - phải implement ở subclass"""
        pass
    
    async def heartbeat(self):
        """Gửi heartbeat đến orchestrator"""
        while self.is_running:
            # Simulate sending heartbeat
            logger.debug(f"[{self.config.name}] Heartbeat - Load: {self.current_load:.1f}%")
            await asyncio.sleep(10)
    
    async def fetch_task(self) -> Optional[dict]:
        """Lấy task từ orchestrator"""
        # In production: call orchestrator API
        # return await self.api_client.get(f"{self.orchestrator_url}/tasks/next")
        return None
    
    async def report_result(self, task_id: str, success: bool, result: dict):
        """Báo cáo kết quả về orchestrator"""
        logger.info(
            f"[{self.config.name}] Task {task_id}: "
            f"{'SUCCESS' if success else 'FAILED'}"
        )
        # In production: POST to orchestrator
        # await self.api_client.post(f"{self.orchestrator_url}/tasks/{task_id}/result", ...)
    
    def update_load(self):
        """Cập nhật current load"""
        # Simulate load calculation
        self.current_load = min(
            100.0,
            (self.tasks_processed % 100) * random.uniform(0.5, 1.5)
        )
    
    async def run(self):
        """Main worker loop"""
        self.is_running = True
        logger.info(f"[{self.config.name}] Worker started")
        
        # Start heartbeat
        asyncio.create_task(self.heartbeat())
        
        while self.is_running:
            try:
                # Check if overloaded
                if self.current_load > self.config.max_load:
                    logger.warning(f"[{self.config.name}] OVERLOADED! Load: {self.current_load:.1f}%")
                    await asyncio.sleep(2)  # Back off
                    continue
                
                # Fetch task from orchestrator
                task = await self.fetch_task()
                if not task:
                    await asyncio.sleep(0.5)
                    continue
                
                # Process task
                task_id = task.get('id', 'unknown')
                task_data = task.get('data', {})
                
                try:
                    # Simulate processing
                    await asyncio.sleep(self.config.processing_delay)
                    
                    # Simulate occasional failure
                    if random.random() < self.config.error_rate:
                        raise Exception("Simulated processing error")
                    
                    result = await self.process_task(task_data)
                    
                    self.tasks_processed += 1
                    self.update_load()
                    
                    await self.report_result(task_id, True, result)
                    
                except Exception as e:
                    self.tasks_failed += 1
                    logger.error(f"[{self.config.name}] Task {task_id} failed: {e}")
                    await self.report_result(task_id, False, {'error': str(e)})
            
            except Exception as e:
                logger.error(f"[{self.config.name}] Worker error: {e}")
                await asyncio.sleep(1)


class CaptureAgent(BaseWorkerAgent):
    """
    Capture Agent - Thu thập dữ liệu từ nguồn (RTSP, webcam, files)
    """
    
    async def process_task(self, task_data: dict) -> dict:
        """Capture image/video từ source"""
        source = task_data.get('source', 'unknown')
        
        logger.info(f"[{self.config.name}] Capturing from {source}")
        
        # Simulate capture
        result = {
            'source': source,
            'timestamp': time.time(),
            'frames_captured': random.randint(1, 30),
            'format': 'jpg',
            'resolution': '1920x1080'
        }
        
        return result


class TransformAgent(BaseWorkerAgent):
    """
    Transform Agent - Xử lý và chuyển đổi dữ liệu
    (resize, anonymize, format conversion, etc.)
    """
    
    async def process_task(self, task_data: dict) -> dict:
        """Transform image data"""
        logger.info(f"[{self.config.name}] Transforming data")
        
        # Simulate transformation operations
        operations = ['resize', 'anonymize', 'compress']
        
        result = {
            'input': task_data,
            'operations_applied': operations,
            'output_size': random.randint(100000, 500000),  # bytes
            'processing_time': self.config.processing_delay,
            'transformed_at': time.time()
        }
        
        return result


class ValidationAgent(BaseWorkerAgent):
    """
    Validation Agent - Kiểm tra chất lượng và tính hợp lệ của dữ liệu
    """
    
    async def process_task(self, task_data: dict) -> dict:
        """Validate data quality"""
        logger.info(f"[{self.config.name}] Validating data")
        
        # Simulate validation checks
        checks = {
            'format_valid': True,
            'size_valid': True,
            'quality_score': random.uniform(0.7, 1.0),
            'privacy_compliant': True
        }
        
        is_valid = all([
            checks['format_valid'],
            checks['size_valid'],
            checks['quality_score'] > 0.6
        ])
        
        result = {
            'checks': checks,
            'is_valid': is_valid,
            'validated_at': time.time()
        }
        
        return result


class StorageAgent(BaseWorkerAgent):
    """
    Storage Agent - Lưu trữ dữ liệu vào S3/MinIO và metadata vào DB
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_backend = "minio"  # hoặc "s3"
        self.metadata_db = "postgresql"
    
    async def process_task(self, task_data: dict) -> dict:
        """Store data và metadata"""
        logger.info(f"[{self.config.name}] Storing to {self.storage_backend}")
        
        # Simulate storage operations
        file_path = f"images/{time.time()}.jpg"
        
        # Store file
        storage_result = {
            'backend': self.storage_backend,
            'path': file_path,
            'size': task_data.get('output_size', 0),
            'stored_at': time.time()
        }
        
        # Store metadata
        metadata = {
            'source_id': task_data.get('source', 'unknown'),
            'timestamp': time.time(),
            'file_path': file_path,
            'size': storage_result['size'],
            'hash': f"sha256_{random.randint(1000000, 9999999)}",
            'privacy_tag': 'anonymized'
        }
        
        result = {
            'storage': storage_result,
            'metadata': metadata,
            'db': self.metadata_db
        }
        
        return result


# Example usage
if __name__ == "__main__":
    async def demo():
        # Create workers
        capture = CaptureAgent(
            WorkerConfig(name="capture-agent-1", processing_delay=0.3)
        )
        
        transform = TransformAgent(
            WorkerConfig(name="transform-agent-1", processing_delay=0.5)
        )
        
        storage = StorageAgent(
            WorkerConfig(name="storage-agent-1", processing_delay=0.2)
        )
        
        # Simulate processing pipeline
        logger.info("=== Starting Pipeline Demo ===")
        
        # Capture
        capture_task = {'source': 'rtsp://camera1.local/stream'}
        capture_result = await capture.process_task(capture_task)
        print(f"Capture result: {capture_result}")
        
        # Transform
        transform_result = await transform.process_task(capture_result)
        print(f"Transform result: {transform_result}")
        
        # Validate
        validator = ValidationAgent(
            WorkerConfig(name="validation-agent-1", processing_delay=0.1)
        )
        validation_result = await validator.process_task(transform_result)
        print(f"Validation result: {validation_result}")
        
        # Store
        if validation_result['is_valid']:
            storage_result = await storage.process_task(transform_result)
            print(f"Storage result: {storage_result}")
        
        logger.info("=== Pipeline Demo Complete ===")
    
    asyncio.run(demo())