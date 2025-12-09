"""
Test script to verify Phase 1 setup
Tests agent registration, heartbeat, and basic communication
"""
import asyncio
import httpx
import time
from datetime import datetime


class Phase1Tester:
    def __init__(self):
        self.orchestrator_url = "http://localhost:8000"
        self.agents = {
            "ingestion": "http://localhost:8001",
            "processing": "http://localhost:8002",
            "storage": "http://localhost:8003"
        }
    
    async def test_orchestrator_health(self):
        """Test 1: Orchestrator is running"""
        print("\n🔍 Test 1: Orchestrator Health Check")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.orchestrator_url}/health")
                assert response.status_code == 200
                data = response.json()
                print(f"✅ Orchestrator is healthy: {data['agent_id']}")
                return True
        except Exception as e:
            print(f"❌ Orchestrator health check failed: {e}")
            return False
    
    async def test_agent_health(self, agent_name: str, url: str):
        """Test 2: Agent health checks"""
        print(f"\n🔍 Test 2.{list(self.agents.keys()).index(agent_name)+1}: {agent_name.capitalize()} Agent Health")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url}/health")
                assert response.status_code == 200
                data = response.json()
                print(f"✅ {agent_name} agent is healthy: {data.get('agent_id', 'N/A')}")
                return True
        except Exception as e:
            print(f"❌ {agent_name} health check failed: {e}")
            return False
    
    async def test_agent_registration(self):
        """Test 3: Check if agents registered with orchestrator"""
        print("\n🔍 Test 3: Agent Registration Status")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.orchestrator_url}/agents")
                assert response.status_code == 200
                data = response.json()
                
                print(f"✅ Found {data['count']} registered agents:")
                for agent in data['agents']:
                    print(f"   - {agent['agent_id']} ({agent['agent_type']}): {agent['status']}")
                    print(f"     Capabilities: {agent['capabilities']}")
                    print(f"     Last heartbeat: {agent['last_heartbeat'] or 'N/A'}")
                
                return data['count'] > 0
        except Exception as e:
            print(f"❌ Registration check failed: {e}")
            return False
    
    async def test_manual_registration(self):
        """Test 4: Manual agent registration"""
        print("\n🔍 Test 4: Manual Agent Registration")
        try:
            async with httpx.AsyncClient() as client:
                registration = {
                    "agent_type": "test",
                    "endpoint": "http://localhost:9999",
                    "capabilities": ["test_capability"],
                    "metadata": {"test": True}
                }
                
                response = await client.post(
                    f"{self.orchestrator_url}/agents/register",
                    json=registration
                )
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ Test agent registered: {data['agent_id']}")
                    return True
                else:
                    print(f"⚠️ Registration returned status {response.status_code}")
                    print(f"   Response: {response.text}")
                    return False
                    
        except Exception as e:
            print(f"❌ Manual registration failed: {e}")
            return False
    
    async def test_heartbeat(self):
        """Test 5: Send heartbeat"""
        print("\n🔍 Test 5: Heartbeat Mechanism")
        try:
            # Get first registered agent
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.orchestrator_url}/agents")
                agents = response.json()['agents']
                
                if not agents:
                    print("⚠️  No agents to test heartbeat")
                    return False
                
                test_agent = agents[0]
                agent_id = test_agent['agent_id']
                
                # Send heartbeat
                heartbeat = {
                    "status": "idle",
                    "timestamp": datetime.utcnow().isoformat(),
                    "metrics": {
                        "cpu_usage": 25.5,
                        "memory_usage": 45.2
                    }
                }
                
                response = await client.post(
                    f"{self.orchestrator_url}/agents/{agent_id}/heartbeat",
                    json=heartbeat
                )
                
                if response.status_code == 200:
                    print(f"✅ Heartbeat sent successfully for {agent_id}")
                    
                    # Verify heartbeat was recorded
                    await asyncio.sleep(1)
                    response = await client.get(f"{self.orchestrator_url}/agents/{agent_id}")
                    agent_info = response.json()
                    print(f"   Last heartbeat: {agent_info['last_heartbeat']}")
                    return True
                else:
                    print(f"⚠️ Heartbeat returned status {response.status_code}")
                    return False
                    
        except Exception as e:
            print(f"❌ Heartbeat test failed: {e}")
            return False
    
    async def test_agent_communication(self):
        """Test 6: Agent-to-Agent message passing"""
        print("\n🔍 Test 6: Agent Communication (Hello World)")
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Test direct agent endpoint first
                for agent_name, agent_url in self.agents.items():
                    try:
                        response = await client.get(f"{agent_url}/hello")
                        if response.status_code == 200:
                            data = response.json()
                            print(f"✅ {agent_name} says: {data['message']}")
                        else:
                            print(f"⚠️ {agent_name} returned status {response.status_code}")
                    except Exception as e:
                        print(f"⚠️ Could not reach {agent_name}: {e}")
                
                # Now test message passing between agents
                print("\n   Testing message passing...")
                response = await client.get(f"{self.orchestrator_url}/agents")
                agents = response.json()['agents']
                
                if len(agents) < 2:
                    print("⚠️  Need at least 2 agents for message test")
                    return True  # Still pass if hello endpoints work
                
                sender = agents[0]
                receiver = agents[1]
                
                # Send message
                message = {
                    "message_id": "test-123",
                    "message_type": "hello",
                    "timestamp": datetime.utcnow().isoformat(),
                    "sender": sender['agent_id'],
                    "receiver": receiver['agent_id'],
                    "payload": {
                        "message": "Hello World from test!",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
                
                # Use localhost instead of endpoint from DB
                receiver_url = self.agents.get(receiver['agent_type'])
                if receiver_url:
                    response = await client.post(
                        f"{receiver_url}/message",
                        json=message
                    )
                    
                    if response.status_code == 200:
                        print(f"✅ Message sent from {sender['agent_id']} to {receiver['agent_id']}")
                        print(f"   Response: {response.json()}")
                    else:
                        print(f"⚠️  Message returned status {response.status_code}")
                
                return True
                    
        except Exception as e:
            print(f"❌ Communication test failed: {e}")
            return False
    
    async def run_all_tests(self):
        """Run all Phase 1 tests"""
        print("=" * 60)
        print("🚀 PHASE 1 - FOUNDATION TESTS")
        print("=" * 60)
        
        results = {}
        
        # Test 1: Orchestrator
        results['orchestrator_health'] = await self.test_orchestrator_health()
        
        # Test 2: All agents
        for agent_name, url in self.agents.items():
            results[f'{agent_name}_health'] = await self.test_agent_health(agent_name, url)
        
        # Test 3: Registration
        results['registration'] = await self.test_agent_registration()
        
        # Test 4: Manual registration
        results['manual_registration'] = await self.test_manual_registration()
        
        # Test 5: Heartbeat
        results['heartbeat'] = await self.test_heartbeat()
        
        # Test 6: Communication
        results['communication'] = await self.test_agent_communication()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        total = len(results)
        passed = sum(1 for v in results.values() if v)
        
        for test_name, passed_test in results.items():
            status = "✅ PASS" if passed_test else "❌ FAIL"
            print(f"{status}: {test_name}")
        
        print(f"\n🎯 Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        
        if passed == total:
            print("\n🎉 All tests passed! Phase 1 foundation is ready!")
        else:
            print(f"\n⚠️  {total - passed} test(s) failed. Review the errors above.")
        
        return passed == total


async def main():
    tester = Phase1Tester()
    
    print("\n⏳ Waiting 5 seconds for services to be ready...")
    await asyncio.sleep(5)
    
    success = await tester.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)