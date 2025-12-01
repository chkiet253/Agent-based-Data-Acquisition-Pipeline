import asyncio
import aiohttp
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "http://localhost:8000/api"


async def register_workers():
    """Đăng ký tất cả workers"""
    workers = [
        {"worker_id": "capture-agent-1", "worker_type": "capture"},
        {"worker_id": "transform-agent-1", "worker_type": "transform"},
        {"worker_id": "validation-agent-1", "worker_type": "validation"},
        {"worker_id": "storage-agent-1", "worker_type": "storage"}
    ]
    
    async with aiohttp.ClientSession() as session:
        for worker in workers:
            async with session.post(
                f"{API_BASE}/workers/register",
                json=worker
            ) as resp:
                result = await resp.json()
                logger.info(f"✅ Registered: {worker['worker_id']}")


async def submit_tasks(num_tasks: int = 10):
    """Submit nhiều tasks"""
    tasks = []
    
    for i in range(num_tasks):
        tasks.append({
            "source": f"rtsp://camera{i}.local/stream",
            "data": {
                "camera_id": i,
                "resolution": "1920x1080",
                "fps": 30
            },
            "priority": 1,
            "max_retries": 3
        })
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{API_BASE}/tasks/batch",
            json=tasks
        ) as resp:
            result = await resp.json()
            logger.info(f"📦 Submitted {result['total_submitted']} tasks")
            return result


async def get_system_status():
    """Lấy trạng thái hệ thống"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_BASE}/status") as resp:
            status = await resp.json()
            logger.info(f"📊 System Status:")
            logger.info(f"   Queue: {status['queue_size']}")
            logger.info(f"   Workers: {status['active_workers']}/{status['total_workers']}")
            logger.info(f"   Metrics: {status['metrics']}")
            return status


async def monitor_metrics(duration: int = 30):
    """Monitor metrics trong một khoảng thời gian"""
    async with aiohttp.ClientSession() as session:
        for i in range(duration):
            async with session.get(f"{API_BASE}/metrics") as resp:
                metrics = await resp.json()
                logger.info(f"[{i}s] Queue: {metrics['orchestrator']['queue_size']}, "
                          f"Completed: {metrics['tasks']['completed']}")
            await asyncio.sleep(1)


async def demo():
    """Chạy demo đầy đủ"""
    logger.info("=" * 50)
    logger.info("🎬 Starting Pipeline Demo")
    logger.info("=" * 50)
    
    # 1. Đăng ký workers
    logger.info("\n📝 Step 1: Registering workers...")
    await register_workers()
    await asyncio.sleep(1)
    
    # 2. Submit tasks
    logger.info("\n📝 Step 2: Submitting tasks...")
    await submit_tasks(20)
    await asyncio.sleep(1)
    
    # 3. Check status
    logger.info("\n📝 Step 3: Checking system status...")
    await get_system_status()
    
    # 4. Monitor
    logger.info("\n📝 Step 4: Monitoring for 10 seconds...")
    await monitor_metrics(10)
    
    logger.info("\n✅ Demo complete!")


if __name__ == "__main__":
    asyncio.run(demo())