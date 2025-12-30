"""
Quick Phase 4 Demo Script
Tests key Phase 4 features quickly
"""
import asyncio
import httpx
import webbrowser
from pathlib import Path


async def quick_demo():
    """Quick demonstration of Phase 4 features"""
    
    print("="*60)
    print("🚀 PHASE 4 - QUICK DEMO")
    print("="*60)
    
    base_urls = {
        'orchestrator': 'http://localhost:8000',
        'ingestion': 'http://localhost:8001',
        'processing': 'http://localhost:8002',
        'storage': 'http://localhost:8003'
    }
    
    print("\n⏳ Waiting for services to start...")
    await asyncio.sleep(5)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: Check all agents are up
        print("\n" + "="*60)
        print("TEST 1: Agent Health Check")
        print("="*60)
        
        all_healthy = True
        for name, url in base_urls.items():
            try:
                response = await client.get(f"{url}/health")
                if response.status_code == 200:
                    print(f"✅ {name.capitalize()}: Healthy")
                else:
                    print(f"❌ {name.capitalize()}: Unhealthy")
                    all_healthy = False
            except Exception as e:
                print(f"❌ {name.capitalize()}: Not reachable - {e}")
                all_healthy = False
        
        if not all_healthy:
            print("\n⚠️  Some agents are not healthy. Please check docker-compose logs.")
            return
        
        # Test 2: Open Dashboard
        print("\n" + "="*60)
        print("TEST 2: Dashboard")
        print("="*60)
        
        dashboard_url = f"{base_urls['orchestrator']}/dashboard"
        print(f"\n📊 Opening dashboard at: {dashboard_url}")
        print("   The dashboard should show:")
        print("   - Agent status")
        print("   - Pipeline metrics")
        print("   - Queue status")
        print("   - Autonomy features")
        
        try:
            webbrowser.open(dashboard_url)
            print("✅ Dashboard opened in browser")
        except:
            print(f"⚠️  Please manually open: {dashboard_url}")
        
        await asyncio.sleep(3)
        
        # Test 3: Check Privacy Features
        print("\n" + "="*60)
        print("TEST 3: Privacy Features")
        print("="*60)
        
        try:
            # Add privacy zone
            response = await client.post(
                f"{base_urls['processing']}/process/privacy/zone",
                params={"x": 100, "y": 100, "width": 200, "height": 200}
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Privacy zone added: {data}")
            else:
                print(f"⚠️  Failed to add privacy zone: {response.status_code}")
            
            # Get privacy stats
            response = await client.get(
                f"{base_urls['processing']}/process/privacy/stats"
            )
            
            if response.status_code == 200:
                stats = response.json()
                print(f"✅ Privacy statistics:")
                for key, value in stats.items():
                    print(f"   {key}: {value}")
            
        except Exception as e:
            print(f"❌ Privacy features test failed: {e}")
        
        # Test 4: Check Agent Registration
        print("\n" + "="*60)
        print("TEST 4: Agent Registration")
        print("="*60)
        
        try:
            response = await client.get(f"{base_urls['orchestrator']}/agents")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Found {data['count']} registered agents:")
                
                for agent in data['agents']:
                    print(f"\n   🤖 {agent['agent_type'].upper()}")
                    print(f"      ID: {agent['agent_id']}")
                    print(f"      Status: {agent['status']}")
                    print(f"      Capabilities: {len(agent['capabilities'])} features")
                    
                    # Check for Phase 4 capabilities
                    phase4_caps = [
                        'multi_method_privacy',
                        'privacy_zones'
                    ]
                    
                    has_phase4 = any(cap in agent['capabilities'] for cap in phase4_caps)
                    if has_phase4:
                        print(f"      Phase 4: ✅ Enhanced")
            
        except Exception as e:
            print(f"❌ Registration check failed: {e}")
        
        # Test 5: Test Video Processing (if available)
        test_video = Path(r"D:\projects\seminar\test_data\video_traffic.mp4")
        
        if test_video.exists():
            print("\n" + "="*60)
            print("TEST 5: Quick Video Processing Test")
            print("="*60)
            
            print(f"\n📥 Starting ingestion with privacy features...")
            
            config = {
                "source_id": "quick_demo",
                "video_path": "/test_data/video_traffic.mp4",
                "fps": 10,
                "batch_size": 5,
                "end_frame": 30,  # Just 30 frames for quick test
                "adaptive_mode": True
            }
            
            try:
                response = await client.post(
                    f"{base_urls['ingestion']}/ingest/start",
                    json=config
                )
                
                if response.status_code == 200:
                    job_data = response.json()
                    job_id = job_data['job_id']
                    print(f"✅ Ingestion started: {job_id}")
                    
                    # Monitor for 15 seconds
                    print(f"\n📊 Monitoring progress (15 seconds)...")
                    
                    for i in range(5):
                        await asyncio.sleep(3)
                        
                        # Check status
                        ing_resp = await client.get(
                            f"{base_urls['ingestion']}/ingest/{job_id}/status"
                        )
                        proc_resp = await client.get(
                            f"{base_urls['processing']}/process/status"
                        )
                        
                        if ing_resp.status_code == 200 and proc_resp.status_code == 200:
                            ing_data = ing_resp.json()
                            proc_data = proc_resp.json()
                            
                            print(f"   [{i*3}s] Ingested: {ing_data.get('frames_ingested', 0)}, "
                                  f"Processed: {proc_data.get('processed_frames', 0)}, "
                                  f"Queue: {proc_data.get('queue_length', 0)}")
                        
                        if ing_data.get('status') == 'completed':
                            print(f"✅ Processing completed!")
                            break
                    
                    # Stop job
                    await client.post(f"{base_urls['ingestion']}/ingest/{job_id}/stop")
                    
                else:
                    print(f"❌ Failed to start ingestion: {response.status_code}")
            
            except Exception as e:
                print(f"❌ Video processing test failed: {e}")
        
        else:
            print("\n" + "="*60)
            print("TEST 5: Video Processing (SKIPPED)")
            print("="*60)
            print(f"⚠️  Test video not found at: {test_video}")
            print("   Place a video file to enable this test")
    
    # Summary
    print("\n" + "="*60)
    print("📋 DEMO SUMMARY")
    print("="*60)
    
    print("\n✅ Phase 4 Features Demonstrated:")
    print("   1. Agent health monitoring")
    print("   2. Web dashboard (check your browser)")
    print("   3. Privacy features (zones & stats)")
    print("   4. Enhanced agent registration")
    if test_video.exists():
        print("   5. Video processing with monitoring")
    
    print("\n📊 Next Steps:")
    print("   1. Keep dashboard open to see real-time updates")
    print("   2. Run full stress tests: python test/test_stress_phase4.py")
    print("   3. Try different privacy methods (blur, pixelate, black_box)")
    print("   4. Test with multiple concurrent streams")
    
    print("\n🔗 Useful URLs:")
    print(f"   Dashboard:     {base_urls['orchestrator']}/dashboard")
    print(f"   API Docs:      {base_urls['orchestrator']}/docs")
    print(f"   Health Check:  {base_urls['orchestrator']}/health")
    print(f"   Agent List:    {base_urls['orchestrator']}/agents")
    
    print("\n" + "="*60)
    print("✅ Phase 4 Quick Demo Completed!")
    print("="*60)


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║       PHASE 4 - ADVANCED FEATURES QUICK DEMO            ║
    ║                                                          ║
    ║  This script will:                                       ║
    ║  • Check agent health                                    ║
    ║  • Open monitoring dashboard                             ║
    ║  • Test privacy features                                 ║
    ║  • Run quick video processing test                       ║
    ║                                                          ║
    ║  Requirements:                                           ║
    ║  • Docker containers running (docker-compose up -d)      ║
    ║  • All agents registered and healthy                     ║
    ║                                                          ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        asyncio.run(quick_demo())
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()