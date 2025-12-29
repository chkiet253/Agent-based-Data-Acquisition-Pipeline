# agents/processing_agent/processing_agent.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from processing_agent.main import ProcessingAgent

if __name__ == "__main__":
    agent = ProcessingAgent(  # ✅ CORRECT
        port=8002,
        orchestrator_url="http://orchestrator:8000"
    )
    agent.run()