from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/agents")
def get_agent_info():
    return {
        "agentId": "ingestion-001",
        "type": "ingestion",
        "capabilities": ["capture_video", "queue_frames"]
    }

@app.post("/invoke/start_capture")
def start_capture(source_id: str, fps: int = 10, duration: int = 60):
    # TODO: Actual video capture logic
    return {
        "status": "started",
        "job_id": "job-123",
        "message": f"Capturing {source_id} at {fps}fps for {duration}s"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)