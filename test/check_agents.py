"""
Check agent registration và fix nếu cần
"""
import asyncio
import httpx


async def check_and_fix():
    """Kiểm tra và fix agent registration"""
    
    print("🔍 Checking agent registration...\n")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Check orchestrator
        try:
            response = await client.get("http://localhost:8000/health")
            print("✅ Orchestrator: Running")
        except:
            print("❌ Orchestrator: Not running")
            print("   Run: docker-compose up -d")
            return False
        
        # Check registered agents
        try:
            response = await client.get("http://localhost:8000/agents")
            agents = response.json()['agents']
            
            print(f"\n📋 Registered agents: {len(agents)}")
            
            if len(agents) == 0:
                print("   ⚠️  NO AGENTS REGISTERED!")
                print("\n🔧 This is why dashboard shows 'No agents registered'")
                print("\n📝 Solution: Restart containers to trigger registration")
                print("   1. docker-compose down")
                print("   2. docker-compose up -d")
                print("   3. Wait 10 seconds")
                print("   4. Refresh dashboard")
                return False
            
            for agent in agents:
                print(f"   • {agent['agent_type']}: {agent['agent_id']}")
                print(f"     Status: {agent['status']}")
                print(f"     Last heartbeat: {agent['last_heartbeat'] or 'Never'}")
            
            # Check individual agents
            print("\n🔍 Checking agent endpoints...")
            
            endpoints = {
                'ingestion': 'http://localhost:8001/health',
                'processing': 'http://localhost:8002/health',
                'storage': 'http://localhost:8003/health'
            }
            
            all_ok = True
            for name, url in endpoints.items():
                try:
                    response = await client.get(url)
                    if response.status_code == 200:
                        print(f"   ✅ {name}")
                    else:
                        print(f"   ⚠️  {name}: HTTP {response.status_code}")
                        all_ok = False
                except Exception as e:
                    print(f"   ❌ {name}: Not reachable")
                    all_ok = False
            
            if not all_ok:
                print("\n⚠️  Some agents running but not reachable")
                print("   This can cause registration issues")
            
            # Check if agents are actually registered
            agent_types = {a['agent_type'] for a in agents}
            required = {'ingestion', 'processing', 'storage'}
            missing = required - agent_types
            
            if missing:
                print(f"\n⚠️  Missing agent types: {missing}")
                print("\n🔧 Fix: Restart those containers")
                for agent_type in missing:
                    print(f"   docker-compose restart {agent_type}")
                return False
            
            print("\n✅ All agents registered correctly!")
            
            # Check metrics
            print("\n📊 Checking dashboard metrics...")
            response = await client.get("http://localhost:8000/api/metrics/summary")
            if response.status_code == 200:
                metrics = response.json()
                print(f"   Pipeline metrics:")
                print(f"     Ingested: {metrics['pipeline']['ingested']}")
                print(f"     Processed: {metrics['pipeline']['processed']}")
                print(f"     Stored: {metrics['pipeline']['stored']}")
                
                if metrics['pipeline']['processed'] == 0:
                    print("\n   ℹ️  No data processed yet")
                    print("   Start streaming: python test/continuous_stream.py")
            
            return True
            
        except Exception as e:
            print(f"❌ Error checking agents: {e}")
            return False


async def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║         AGENT REGISTRATION CHECKER                       ║
║  Diagnose why dashboard shows no data                    ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    success = await check_and_fix()
    
    print("\n" + "="*60)
    if success:
        print("✅ SYSTEM OK")
        print("\n📊 Dashboard should show data at:")
        print("   http://localhost:8000/dashboard")
        print("\n🚀 Start streaming to see real-time updates:")
        print("   python test/continuous_stream.py")
    else:
        print("⚠️  SYSTEM NEEDS ATTENTION")
        print("\n🔧 Recommended fix:")
        print("   docker-compose down")
        print("   docker-compose up -d")
        print("   python test/check_agents.py  # Run this again")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())