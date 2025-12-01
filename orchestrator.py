# orchestrator.py - Host/Orchestrator Agent
import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from collections import deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    OVERLOADED = "overloaded"
    FAILED = "failed"
    RECOVERING = "recovering"


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class Task:
    id: str
    data: dict
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)
    assigned_to: Optional[str] = None


@dataclass
class AgentMetrics:
    load: float = 0.0
    tasks_processed: int = 0
    tasks_failed: int = 0
    queue_size: int = 0
    last_heartbeat: datetime = field(default_factory=datetime.now)


class BackpressurePolicy:
    """Quản lý backpressure để tránh overload"""
    
    def __init__(self, max_queue_size: int = 100, high_watermark: int = 70):
        self.max_queue_size = max_queue_size
        self.high_watermark = high_watermark
        self.throttle_factor = 1.0
    
    def should_throttle(self, queue_size: int) -> bool:
        """Kiểm tra có cần throttle không"""
        if queue_size >= self.max_queue_size:
            self.throttle_factor = 0.1
            return True
        elif queue_size >= self.high_watermark:
            self.throttle_factor = 0.5
            return True
        else:
            self.throttle_factor = 1.0
            return False
    
    def get_batch_size(self, base_size: int, queue_size: int) -> int:
        """Tính batch size dựa trên queue"""
        self.should_throttle(queue_size)
        return max(1, int(base_size * self.throttle_factor))


class Orchestrator:
    """
    Host/Orchestrator Agent - Điều phối toàn bộ pipeline
    """
    
    def __init__(self, name: str = "orchestrator"):
        self.name = name
        self.status = AgentStatus.IDLE
        self.task_queue: deque = deque()
        self.workers: Dict[str, AgentMetrics] = {}
        self.backpressure = BackpressurePolicy()
        self.retry_backoff_base = 2  # exponential backoff
        self.metrics = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'retries': 0
        }
    
    def register_worker(self, worker_id: str):
        """Đăng ký worker mới"""
        self.workers[worker_id] = AgentMetrics()
        logger.info(f"[{self.name}] Registered worker: {worker_id}")
    
    def submit_task(self, task: Task):
        """Submit task vào queue"""
        if len(self.task_queue) >= self.backpressure.max_queue_size:
            logger.warning(f"[{self.name}] Queue full! Rejecting task {task.id}")
            return False
        
        self.task_queue.append(task)
        self.metrics['total_tasks'] += 1
        
        # Apply backpressure nếu cần
        if self.backpressure.should_throttle(len(self.task_queue)):
            logger.warning(
                f"[{self.name}] BACKPRESSURE APPLIED! Queue size: {len(self.task_queue)}"
            )
        
        return True
    
    def get_next_task(self, worker_id: str) -> Optional[Task]:
        """Worker lấy task tiếp theo"""
        if not self.task_queue:
            return None
        
        # Kiểm tra worker health
        if worker_id not in self.workers:
            logger.error(f"[{self.name}] Unknown worker: {worker_id}")
            return None
        
        task = self.task_queue.popleft()
        task.assigned_to = worker_id
        task.status = TaskStatus.PROCESSING
        
        # Update worker metrics
        self.workers[worker_id].queue_size = len(self.task_queue)
        
        logger.info(f"[{self.name}] Assigned task {task.id} to {worker_id}")
        return task
    
    def report_task_result(self, task_id: str, success: bool, worker_id: str):
        """Worker báo cáo kết quả"""
        worker = self.workers.get(worker_id)
        if not worker:
            return
        
        if success:
            self.metrics['completed_tasks'] += 1
            worker.tasks_processed += 1
            logger.info(f"[{self.name}] Task {task_id} completed by {worker_id}")
        else:
            self.metrics['failed_tasks'] += 1
            worker.tasks_failed += 1
            logger.error(f"[{self.name}] Task {task_id} failed on {worker_id}")
    
    def should_retry_task(self, task: Task) -> bool:
        """Quyết định có retry task không"""
        return task.retry_count < task.max_retries
    
    def schedule_retry(self, task: Task):
        """Schedule retry với exponential backoff"""
        if self.should_retry_task(task):
            task.retry_count += 1
            task.status = TaskStatus.RETRY
            
            # Exponential backoff
            delay = self.retry_backoff_base ** task.retry_count
            
            logger.info(
                f"[{self.name}] Scheduling retry for task {task.id} "
                f"(attempt {task.retry_count}/{task.max_retries}) "
                f"after {delay}s"
            )
            
            self.metrics['retries'] += 1
            # Đưa lại vào queue sau delay
            self.task_queue.append(task)
            return True
        else:
            logger.error(
                f"[{self.name}] Task {task.id} exceeded max retries"
            )
            return False
    
    def get_status(self) -> dict:
        """Lấy trạng thái hệ thống"""
        return {
            'orchestrator': {
                'status': self.status.value,
                'queue_size': len(self.task_queue),
                'metrics': self.metrics,
                'backpressure': {
                    'active': self.backpressure.should_throttle(len(self.task_queue)),
                    'throttle_factor': self.backpressure.throttle_factor
                }
            },
            'workers': {
                worker_id: {
                    'load': metrics.load,
                    'processed': metrics.tasks_processed,
                    'failed': metrics.tasks_failed,
                    'queue_size': metrics.queue_size
                }
                for worker_id, metrics in self.workers.items()
            }
        }
    
    async def monitor_workers(self):
        """Monitor worker health"""
        while True:
            await asyncio.sleep(5)
            now = datetime.now()
            
            for worker_id, metrics in self.workers.items():
                # Check heartbeat timeout
                if (now - metrics.last_heartbeat).seconds > 30:
                    logger.warning(
                        f"[{self.name}] Worker {worker_id} timeout - initiating recovery"
                    )
                    # TODO: Restart worker or reassign tasks
    
    async def run(self):
        """Main orchestrator loop"""
        self.status = AgentStatus.RUNNING
        logger.info(f"[{self.name}] Orchestrator started")
        
        # Start monitoring
        asyncio.create_task(self.monitor_workers())
        
        while self.status == AgentStatus.RUNNING:
            await asyncio.sleep(1)
            
            # Log metrics periodically
            if self.metrics['total_tasks'] % 100 == 0:
                logger.info(f"[{self.name}] Status: {self.get_status()}")


# Example usage
if __name__ == "__main__":
    async def demo():
        orchestrator = Orchestrator()
        
        # Register workers
        orchestrator.register_worker("capture-agent-1")
        orchestrator.register_worker("transform-agent-1")
        orchestrator.register_worker("storage-agent-1")
        
        # Submit some tasks
        for i in range(10):
            task = Task(
                id=f"task-{i}",
                data={"image_url": f"https://example.com/image{i}.jpg"}
            )
            orchestrator.submit_task(task)
        
        # Simulate worker processing
        for worker_id in orchestrator.workers.keys():
            task = orchestrator.get_next_task(worker_id)
            if task:
                # Simulate success/failure
                success = i % 7 != 0  # Fail every 7th task
                orchestrator.report_task_result(task.id, success, worker_id)
                
                if not success:
                    orchestrator.schedule_retry(task)
        
        print("\n=== System Status ===")
        import json
        print(json.dumps(orchestrator.get_status(), indent=2))
    
    asyncio.run(demo())