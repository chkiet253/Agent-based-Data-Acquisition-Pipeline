"""
Quick Dashboard Verification Script
"""
import asyncio
import httpx

async def verify_dashboard():
    print("🔍 Verifying Dashboard Setup...")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        
        # Test 1: Check orchestrator health
        print("\n1. Checking orchestrator health...")
        try:
            response = await client.get("http://localhost:8000/health")
            if response.status_code == 200:
                print("   ✅ Orchestrator is running")
            else:
                print(f"   ❌ Orchestrator returned: {response.status_code}")
                return
        except Exception as e:
            print(f"   ❌ Cannot reach orchestrator: {e}")
            return
        
        # Test 2: Check if dashboard endpoint exists
        print("\n2. Checking dashboard endpoint...")
        try:
            response = await client.get("http://localhost:8000/dashboard")
            if response.status_code == 200:
                print("   ✅ Dashboard endpoint exists")
                print(f"   Content type: {response.headers.get('content-type')}")
                print(f"   Content length: {len(response.text)} bytes")
            else:
                print(f"   ❌ Dashboard returned: {response.status_code}")
                print(f"   Detail: {response.text}")
        except Exception as e:
            print(f"   ❌ Dashboard error: {e}")
        
        # Test 3: Check metrics API
        print("\n3. Checking metrics API...")
        try:
            response = await client.get("http://localhost:8000/api/metrics/summary")
            if response.status_code == 200:
                data = response.json()
                print("   ✅ Metrics API working")
                print(f"   Timestamp: {data.get('timestamp')}")
                print(f"   Agents: {len(data.get('agents', []))}")
                print(f"   Pipeline metrics: {data.get('pipeline')}")
            else:
                print(f"   ❌ Metrics API returned: {response.status_code}")
                print(f"   Detail: {response.text}")
        except Exception as e:
            print(f"   ❌ Metrics API error: {e}")
        
        # Test 4: Check agents endpoint
        print("\n4. Checking agents endpoint...")
        try:
            response = await client.get("http://localhost:8000/agents")
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Found {data['count']} registered agents")
                for agent in data['agents']:
                    print(f"      - {agent['agent_type']}: {agent['agent_id']}")
            else:
                print(f"   ❌ Agents endpoint returned: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Agents endpoint error: {e}")
        
        # Test 5: Check processing agent endpoints
        print("\n5. Checking processing agent privacy endpoints...")
        try:
            # Get current status
            response = await client.get("http://localhost:8002/process/status")
            if response.status_code == 200:
                print("   ✅ Processing agent reachable")
            
            # Try to add privacy zone
            response = await client.post(
                "http://localhost:8002/process/privacy/zone",
                params={"x": 100, "y": 100, "width": 200, "height": 200}
            )
            
            if response.status_code == 200:
                print("   ✅ Privacy zone endpoint working")
                print(f"   Response: {response.json()}")
            else:
                print(f"   ❌ Privacy zone returned: {response.status_code}")
                print(f"   Detail: {response.text}")
            
            # Get privacy stats
            response = await client.get("http://localhost:8002/process/privacy/stats")
            if response.status_code == 200:
                print("   ✅ Privacy stats endpoint working")
                print(f"   Stats: {response.json()}")
            else:
                print(f"   ❌ Privacy stats returned: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Processing agent error: {e}")
        
    print("\n" + "=" * 60)
    print("✅ Verification complete!")
    print("\nIf dashboard shows 404:")
    print("  1. Stop containers: docker-compose down")
    print("  2. Rebuild orchestrator: docker-compose build orchestrator")
    print("  3. Restart: docker-compose up -d")
    print("\nIf privacy endpoints fail:")
    print("  1. Rebuild processing: docker-compose build processing")
    print("  2. Restart: docker-compose up -d processing")


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║          DASHBOARD & ENDPOINTS VERIFICATION              ║
╚══════════════════════════════════════════════════════════╝
    """)
    asyncio.run(verify_dashboard())