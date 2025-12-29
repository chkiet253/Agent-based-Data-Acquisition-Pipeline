# agents/ingestion_agent/ingestion_agent.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion_agent.main import IngestionAgent

if __name__ == "__main__":
    agent = IngestionAgent(  # ✅ CORRECT
        port=8001,
        orchestrator_url="http://orchestrator:8000"
    )
    agent.run()