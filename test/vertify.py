import asyncio
import httpx

async def verify():
    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8000/agents")
        agents = response.json()['agents']
        
        required = {
            'ingestion': ['adaptive_fps', 'backpressure_handling', 'self_healing'],
            'processing': ['circuit_breaker', 'backpressure_signaling', 'self_healing'],
            'storage': ['fallback_storage', 'auto_cleanup', 'health_monitoring']
        }
        
        print("📋 Capability Check:")
        for agent in agents:
            agent_type = agent['agent_type']
            if agent_type in required:
                caps = set(agent['capabilities'])
                req = set(required[agent_type])
                
                if req.issubset(caps):
                    print(f"✅ {agent_type}: OK")
                else:
                    print(f"❌ {agent_type}: Missing {req - caps}")

asyncio.run(verify())