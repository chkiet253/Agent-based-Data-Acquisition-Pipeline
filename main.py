# main.py - FastAPI Server cho Agent Orchestration System
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime
from enum import Enum
import asyncio
import logging

# Import từ các file khác (orchestrator.py, workers.py)
# from orchestrator import Orchestrator, Task, TaskStatus, AgentStatus
# from workers import CaptureAgent, TransformAgent, ValidationAgent, StorageAgent, WorkerConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Agent-Based Data Pipeline Orchestrator",
    description="Multi-Agent Autonomous Pipeline API",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ Pydantic Models ============

class TaskRequest(BaseModel):
    """Request để tạo task mới"""
    source: str
    data: Dict
    priority: int = 1
    max_retries: int = 3


class TaskResponse(BaseModel):
    """Response chứa thông tin task"""
    id: str
    status: str
    created_at: datetime
    retry_count: int
    assigned_to: Optional[str] = None


class WorkerRegistration(BaseModel):
    """Request để đăng ký worker"""
    worker_id: str
    worker_type: str  # capture, transform, validation, storage
    capabilities: List[str] = []


class SystemStatus(BaseModel):
    """Trạng thái tổng quan hệ thống"""
    orchestrator_status: str
    queue_size: int
    total_workers: int
    active_workers: int
    metrics: Dict
    backpressure_active: bool


# ============ Global State ============
# Trong production nên dùng dependency injection
orchestrator = None
workers = {}
system_running = False


# ============ API Endpoints ============

@app.on_event("startup")
async def startup_event():
    """Khởi tạo hệ thống khi start server"""
    global orchestrator, system_running
    
    # Khởi tạo orchestrator (cần import từ orchestrator.py)
    # orchestrator = Orchestrator()
    # asyncio.create_task(orchestrator.run())
    
    system_running = True
    logger.info("🚀 Agent Orchestration System Started")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup khi tắt server"""
    global system_running
    system_running = False
    logger.info("🛑 Agent Orchestration System Stopped")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Agent-Based Data Pipeline Orchestrator",
        "status": "running" if system_running else "stopped",
        "version": "1.0.0"
    }


@app.get("/api/status", response_model=SystemStatus)
async def get_system_status():
    """Lấy trạng thái tổng quan của hệ thống"""
    # Trong production gọi orchestrator.get_status()
    return SystemStatus(
        orchestrator_status="running",
        queue_size=42,
        total_workers=4,
        active_workers=4,
        metrics={
            "total_tasks": 1250,
            "completed_tasks": 1189,
            "failed_tasks": 15,
            "retries": 23
        },
        backpressure_active=False
    )


@app.post("/api/workers/register")
async def register_worker(registration: WorkerRegistration):
    """Đăng ký worker mới với orchestrator"""
    # orchestrator.register_worker(registration.worker_id)
    
    workers[registration.worker_id] = {
        "type": registration.worker_type,
        "capabilities": registration.capabilities,
        "registered_at": datetime.now(),
        "status": "idle"
    }
    
    logger.info(f"✅ Registered worker: {registration.worker_id}")
    
    return {
        "message": f"Worker {registration.worker_id} registered successfully",
        "worker": workers[registration.worker_id]
    }


@app.get("/api/workers")
async def list_workers():
    """Danh sách tất cả workers"""
    return {
        "total_workers": len(workers),
        "workers": workers
    }


@app.post("/api/tasks", response_model=TaskResponse)
async def submit_task(task_request: TaskRequest):
    """Submit task mới vào pipeline"""
    # task = Task(
    #     id=f"task-{int(time.time() * 1000)}",
    #     data=task_request.data,
    #     max_retries=task_request.max_retries
    # )
    # 
    # success = orchestrator.submit_task(task)
    # if not success:
    #     raise HTTPException(status_code=503, detail="Queue full - backpressure applied")
    
    task_id = f"task-{int(datetime.now().timestamp() * 1000)}"
    
    logger.info(f"📥 Task submitted: {task_id}")
    
    return TaskResponse(
        id=task_id,
        status="pending",
        created_at=datetime.now(),
        retry_count=0
    )


@app.post("/api/tasks/batch")
async def submit_batch_tasks(tasks: List[TaskRequest]):
    """Submit nhiều tasks cùng lúc"""
    results = []
    
    for task_req in tasks:
        task_id = f"task-{int(datetime.now().timestamp() * 1000)}"
        results.append({
            "id": task_id,
            "status": "pending"
        })
    
    logger.info(f"📦 Batch submitted: {len(tasks)} tasks")
    
    return {
        "total_submitted": len(tasks),
        "tasks": results
    }


@app.get("/api/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Lấy trạng thái của một task"""
    # Trong production: query từ orchestrator hoặc database
    return {
        "id": task_id,
        "status": "completed",
        "progress": 100,
        "assigned_to": "capture-agent-1",
        "retry_count": 0,
        "result": {
            "processed": True,
            "output_path": f"s3://bucket/images/{task_id}.jpg"
        }
    }


@app.get("/api/workers/{worker_id}/tasks/next")
async def get_next_task(worker_id: str):
    """Worker lấy task tiếp theo để xử lý"""
    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not found")
    
    # task = orchestrator.get_next_task(worker_id)
    # if not task:
    #     return {"task": None, "message": "No tasks available"}
    
    return {
        "task": {
            "id": f"task-{int(datetime.now().timestamp())}",
            "data": {
                "source": "rtsp://camera1.local/stream",
                "format": "jpg"
            }
        }
    }


@app.post("/api/tasks/{task_id}/result")
async def report_task_result(
    task_id: str,
    worker_id: str,
    success: bool,
    result: Optional[Dict] = None,
    error: Optional[str] = None
):
    """Worker báo cáo kết quả xử lý task"""
    # orchestrator.report_task_result(task_id, success, worker_id)
    
    if success:
        logger.info(f"✅ Task {task_id} completed by {worker_id}")
    else:
        logger.error(f"❌ Task {task_id} failed on {worker_id}: {error}")
        # orchestrator.schedule_retry(task) nếu cần
    
    return {
        "message": "Result recorded",
        "task_id": task_id,
        "success": success
    }


@app.post("/api/workers/{worker_id}/heartbeat")
async def worker_heartbeat(worker_id: str, load: float, queue_size: int):
    """Worker gửi heartbeat"""
    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not found")
    
    workers[worker_id]["last_heartbeat"] = datetime.now()
    workers[worker_id]["load"] = load
    workers[worker_id]["queue_size"] = queue_size
    
    return {"message": "Heartbeat received"}


@app.get("/api/metrics")
async def get_metrics():
    """Lấy metrics chi tiết của hệ thống"""
    return {
        "orchestrator": {
            "queue_size": 42,
            "backpressure_active": False,
            "throttle_factor": 1.0
        },
        "tasks": {
            "total": 1250,
            "completed": 1189,
            "failed": 15,
            "retry": 23,
            "pending": 23
        },
        "workers": {
            worker_id: {
                "load": worker.get("load", 0),
                "queue_size": worker.get("queue_size", 0),
                "status": worker.get("status", "unknown")
            }
            for worker_id, worker in workers.items()
        },
        "performance": {
            "avg_latency_ms": 45.3,
            "throughput_per_sec": 23.5,
            "error_rate_percent": 1.2
        }
    }


@app.post("/api/system/start")
async def start_system(background_tasks: BackgroundTasks):
    """Khởi động toàn bộ pipeline"""
    global system_running
    
    if system_running:
        return {"message": "System already running"}
    
    # background_tasks.add_task(orchestrator.run)
    system_running = True
    
    logger.info("🚀 System started")
    return {"message": "System started successfully"}


@app.post("/api/system/stop")
async def stop_system():
    """Dừng pipeline (graceful shutdown)"""
    global system_running
    
    system_running = False
    # orchestrator.status = AgentStatus.IDLE
    
    logger.info("🛑 System stopped")
    return {"message": "System stopped successfully"}


@app.post("/api/system/reset")
async def reset_system():
    """Reset toàn bộ hệ thống"""
    global workers
    
    workers = {}
    # orchestrator = Orchestrator()  # Tạo mới
    
    logger.info("🔄 System reset")
    return {"message": "System reset successfully"}


# ============ WebSocket cho Real-time Updates (Optional) ============
from fastapi import WebSocket, WebSocketDisconnect

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """WebSocket stream cho real-time metrics"""
    await websocket.accept()
    
    try:
        while True:
            # Gửi metrics mỗi giây
            metrics = {
                "timestamp": datetime.now().isoformat(),
                "queue_size": 42,
                "throughput": 23.5,
                "active_workers": len(workers)
            }
            
            await websocket.send_json(metrics)
            await asyncio.sleep(1)
    
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")


# ============ Run Server ============
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )