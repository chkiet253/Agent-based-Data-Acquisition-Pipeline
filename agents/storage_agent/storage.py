import sys
import os

# Add base agent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'base'))

from base.base_agent import HelloWorldAgent

if __name__ == "__main__":
    agent = HelloWorldAgent(
        agent_type="storage",
        port=8003,
        orchestrator_url="http://orchestrator:8000"
    )
    agent.run()