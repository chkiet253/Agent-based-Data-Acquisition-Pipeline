# agents/storage_agent/storage.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from storage_agent.main import StorageAgent

if __name__ == "__main__":
    agent = StorageAgent(  # ✅ CORRECT
        port=8003,
        orchestrator_url="http://orchestrator:8000"
    )
    agent.run()