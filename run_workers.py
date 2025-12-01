import asyncio
import logging
from workers import (
    CaptureAgent,
    TransformAgent,
    ValidationAgent,
    StorageAgent,
    WorkerConfig
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_all_workers():
    """Chạy tất cả workers đồng thời"""
    
    # Tạo workers
    capture = CaptureAgent(
        WorkerConfig(
            name="capture-agent-1",
            max_load=80.0,
            processing_delay=0.3,
            error_rate=0.02
        )
    )
    
    transform = TransformAgent(
        WorkerConfig(
            name="transform-agent-1",
            max_load=85.0,
            processing_delay=0.5,
            error_rate=0.03
        )
    )
    
    validation = ValidationAgent(
        WorkerConfig(
            name="validation-agent-1",
            max_load=70.0,
            processing_delay=0.2,
            error_rate=0.01
        )
    )
    
    storage = StorageAgent(
        WorkerConfig(
            name="storage-agent-1",
            max_load=75.0,
            processing_delay=0.4,
            error_rate=0.02
        )
    )
    
    logger.info("🚀 Starting all worker agents...")
    
    # Chạy tất cả workers concurrently
    await asyncio.gather(
        capture.run(),
        transform.run(),
        validation.run(),
        storage.run()
    )


if __name__ == "__main__":
    try:
        asyncio.run(run_all_workers())
    except KeyboardInterrupt:
        logger.info("⏸️ Workers stopped by user")