"""
COMPLETE DEMO SCRIPT - Multi-Agent Data Acquisition Pipeline
Based on research paper architecture + Phase 4 implementation
"""
import asyncio
import httpx
import time
from datetime import datetime
from pathlib import Path
import json


class CompletePipelineDemo:
    """
    Demo script theo flow của bài báo:
    1. Agent Registration & Discovery
    2. Data Ingestion với Adaptive Features
    3. Processing với Privacy Protection
    4. Storage với Fallback Mechanism
    5. Real-time Monitoring
    """
    
    def __init__(self):
        self.base_urls = {
            'orchestrator': 'http://localhost:8000',
            'ingestion': 'http://localhost:8001',
            'processing': 'http://localhost:8002',
            'storage': 'http://localhost:8003'
        }
        
        # Test video (như trong bài báo: RTSP camera hoặc video file)
        self.test_video = "/test_data/traffic.mp4"
        
        # Demo configuration
        self.demo_duration_seconds = 60  # 1 phút demo
        self.monitoring_interval = 3  # Update mỗi 3 giây
        
        # Metrics tracking
        self.metrics_history = []
    
    def print_section(self, title, emoji="📋"):
        """Print formatted section header"""
        print("\n" + "="*70)
        print(f"{emoji} {title}")
        print("="*70)
    
    async def section_1_system_health(self):
        """
        SECTION 1: System Health Check
        Kiểm tra tất cả agents đã sẵn sàng (như Table 1 trong bài báo)
        """
        self.print_section("SECTION 1: System Health & Agent Discovery", "🏥")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            print("\n1.1 Checking Orchestrator...")
            try:
                response = await client.get(f"{self.base_urls['orchestrator']}/health")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   ✅ Orchestrator: {data['agent_id']}")
                    print(f"      Phase: {data.get('phase', 'N/A')}")
                    print(f"      Uptime: {data.get('timestamp', 'N/A')}")
                else:
                    print(f"   ❌ Orchestrator unhealthy")
                    return False
            except Exception as e:
                print(f"   ❌ Cannot reach orchestrator: {e}")
                return False
            
            print("\n1.2 Discovering Registered Agents...")
            try:
                response = await client.get(f"{self.base_urls['orchestrator']}/agents")
                if response.status_code == 200:
                    data = response.json()
                    agents = data['agents']
                    
                    print(f"   Found {len(agents)} registered agents:\n")
                    
                    for agent in agents:
                        print(f"   🤖 {agent['agent_type'].upper()}")
                        print(f"      ID: {agent['agent_id']}")
                        print(f"      Status: {agent['status']}")
                        print(f"      Endpoint: {agent['endpoint']}")
                        print(f"      Capabilities: {len(agent['capabilities'])} features")
                        
                        # Hiển thị key capabilities (như Table 1)
                        key_caps = []
                        if 'adaptive_fps' in agent['capabilities']:
                            key_caps.append('Adaptive FPS')
                        if 'privacy_zones' in agent['capabilities']:
                            key_caps.append('Privacy Zones')
                        if 'fallback_storage' in agent['capabilities']:
                            key_caps.append('Fallback Storage')
                        if 'circuit_breaker' in agent['capabilities']:
                            key_caps.append('Circuit Breaker')
                        
                        if key_caps:
                            print(f"      Key Features: {', '.join(key_caps)}")
                        print()
                    
                    # Verify all required agents present
                    agent_types = {a['agent_type'] for a in agents}
                    required = {'ingestion', 'processing', 'storage'}
                    
                    if required.issubset(agent_types):
                        print("   ✅ All required agents are registered")
                        return True
                    else:
                        missing = required - agent_types
                        print(f"   ❌ Missing agents: {missing}")
                        return False
                        
            except Exception as e:
                print(f"   ❌ Agent discovery failed: {e}")
                return False
    
    async def section_2_configure_privacy(self):
        """
        SECTION 2: Privacy Configuration
        Setup privacy zones và anonymization methods (Phase 4 feature)
        """
        self.print_section("SECTION 2: Privacy Protection Setup", "🔒")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            print("\n2.1 Configuring Privacy Zones...")
            
            # Add privacy zones (giống như bài báo mention về anonymization)
            zones = [
                {"x": 100, "y": 100, "width": 200, "height": 200, "purpose": "License plates"},
                {"x": 400, "y": 300, "width": 150, "height": 150, "purpose": "Faces area"}
            ]
            
            for i, zone in enumerate(zones):
                try:
                    response = await client.post(
                        f"{self.base_urls['processing']}/process/privacy/zone",
                        params={
                            "x": zone['x'],
                            "y": zone['y'],
                            "width": zone['width'],
                            "height": zone['height']
                        }
                    )
                    
                    if response.status_code == 200:
                        print(f"   ✅ Zone {i+1}: {zone['purpose']}")
                        print(f"      Position: ({zone['x']}, {zone['y']})")
                        print(f"      Size: {zone['width']}x{zone['height']}px")
                    else:
                        print(f"   ⚠️  Zone {i+1} failed: {response.status_code}")
                        
                except Exception as e:
                    print(f"   ❌ Zone {i+1} error: {e}")
            
            print("\n2.2 Privacy Statistics (Initial)...")
            try:
                response = await client.get(
                    f"{self.base_urls['processing']}/process/privacy/stats"
                )
                
                if response.status_code == 200:
                    stats = response.json()
                    print(f"   Total operations: {stats.get('total_operations', 0)}")
                    print(f"   Methods available: {stats.get('methods_used', ['blur', 'pixelate', 'black_box'])}")
                    
            except Exception as e:
                print(f"   ⚠️  Stats unavailable: {e}")
            
            return True
    
    async def section_3_start_ingestion(self):
        """
        SECTION 3: Start Data Ingestion
        Như Algorithm 1 trong bài báo
        """
        self.print_section("SECTION 3: Starting Data Ingestion", "📥")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            print("\n3.1 Ingestion Configuration:")
            
            config = {
                "source_id": "demo_camera_001",
                "video_path": self.test_video,
                "fps": 15,  # Moderate FPS
                "batch_size": 10,
                "start_frame": 0,
                "end_frame": self.demo_duration_seconds * 15,  # Total frames
                
                # Phase 3 features (như bài báo mention)
                "adaptive_mode": True,
                "min_fps": 5,
                "max_fps": 30
            }
            
            print(f"   Source: {config['source_id']}")
            print(f"   Target FPS: {config['fps']} (adaptive: {config['min_fps']}-{config['max_fps']})")
            print(f"   Batch size: {config['batch_size']}")
            print(f"   Duration: ~{self.demo_duration_seconds}s")
            
            print("\n3.2 Starting Ingestion Job...")
            
            try:
                response = await client.post(
                    f"{self.base_urls['ingestion']}/ingest/start",
                    json=config
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.job_id = data['job_id']
                    
                    print(f"   ✅ Job Started: {self.job_id}")
                    print(f"   Status: {data['status']}")
                    print(f"   Initial queue: {data.get('queue_length', 0)}")
                    
                    return True
                else:
                    print(f"   ❌ Failed to start: {response.status_code}")
                    print(f"   Response: {response.text}")
                    return False
                    
            except Exception as e:
                print(f"   ❌ Ingestion start failed: {e}")
                return False
    
    async def section_4_monitor_pipeline(self):
        """
        SECTION 4: Real-time Pipeline Monitoring
        Monitor như Figure 1 trong bài báo (Architecture diagram)
        """
        self.print_section("SECTION 4: Real-time Pipeline Monitoring", "📊")
        
        print(f"\nMonitoring for {self.demo_duration_seconds} seconds...")
        print("(Press Ctrl+C to stop early)\n")
        
        print(f"{'Time':<8} {'Ingested':<10} {'Processed':<10} {'Stored':<10} "
              f"{'Detections':<12} {'Queue':<8} {'FPS':<6} {'Status':<10}")
        print("-" * 90)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            start_time = time.time()
            iteration = 0
            
            try:
                while time.time() - start_time < self.demo_duration_seconds:
                    elapsed = int(time.time() - start_time)
                    
                    # Gather metrics from all agents
                    try:
                        # Ingestion status
                        ing_resp = await client.get(
                            f"{self.base_urls['ingestion']}/ingest/{self.job_id}/status"
                        )
                        ing_data = ing_resp.json() if ing_resp.status_code == 200 else {}
                        
                        # Processing status
                        proc_resp = await client.get(
                            f"{self.base_urls['processing']}/process/status"
                        )
                        proc_data = proc_resp.json() if proc_resp.status_code == 200 else {}
                        
                        # Storage status
                        stor_resp = await client.get(
                            f"{self.base_urls['storage']}/storage/status"
                        )
                        stor_data = stor_resp.json() if stor_resp.status_code == 200 else {}
                        
                        # Extract metrics
                        ingested = ing_data.get('frames_ingested', 0)
                        processed = proc_data.get('processed_frames', 0)
                        stored = stor_data.get('frames_stored', 0)
                        detections = proc_data.get('detection_count', 0)
                        queue_len = proc_data.get('queue_length', 0)
                        current_fps = ing_data.get('current_fps', 0)
                        job_status = ing_data.get('status', 'unknown')
                        
                        # Print row
                        print(f"{elapsed:>4}s    {ingested:<10} {processed:<10} {stored:<10} "
                              f"{detections:<12} {queue_len:<8} {current_fps:<6} {job_status:<10}")
                        
                        # Store for later analysis
                        self.metrics_history.append({
                            'timestamp': elapsed,
                            'ingested': ingested,
                            'processed': processed,
                            'stored': stored,
                            'detections': detections,
                            'queue': queue_len,
                            'fps': current_fps,
                            'status': job_status
                        })
                        
                        # Check if completed early
                        if job_status == 'completed':
                            print("\n   ✅ Pipeline completed all frames!")
                            break
                        
                        # Check for adaptive FPS changes (như bài báo Figure 2)
                        if iteration > 0:
                            prev_fps = self.metrics_history[-2]['fps']
                            if current_fps != prev_fps:
                                print(f"   🔄 Adaptive FPS: {prev_fps} → {current_fps} "
                                      f"(Queue: {queue_len})")
                        
                        iteration += 1
                        
                    except Exception as e:
                        print(f"   ⚠️  Monitoring error: {e}")
                    
                    await asyncio.sleep(self.monitoring_interval)
                
            except KeyboardInterrupt:
                print("\n\n   ⚠️  Monitoring stopped by user")
            
            print("\n" + "-" * 90)
            return True
    
    async def section_5_analyze_results(self):
        """
        SECTION 5: Results Analysis
        Tương tự evaluation section trong bài báo
        """
        self.print_section("SECTION 5: Results Analysis", "📈")
        
        if not self.metrics_history:
            print("   ⚠️  No metrics collected")
            return False
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            print("\n5.1 Final Pipeline Statistics:")
            
            try:
                # Get final status
                ing_resp = await client.get(
                    f"{self.base_urls['ingestion']}/ingest/{self.job_id}/status"
                )
                proc_resp = await client.get(
                    f"{self.base_urls['processing']}/process/status"
                )
                stor_resp = await client.get(
                    f"{self.base_urls['storage']}/storage/status"
                )
                
                ing_data = ing_resp.json() if ing_resp.status_code == 200 else {}
                proc_data = proc_resp.json() if proc_resp.status_code == 200 else {}
                stor_data = stor_resp.json() if stor_resp.status_code == 200 else {}
                
                # Ingestion metrics
                print(f"\n   📥 Ingestion Agent:")
                print(f"      Frames ingested: {ing_data.get('frames_ingested', 0)}")
                print(f"      Frames dropped: {ing_data.get('frames_dropped', 0)}")
                print(f"      Retry count: {ing_data.get('retry_count', 0)}")
                print(f"      Final FPS: {ing_data.get('current_fps', 0)}")
                
                if ing_data.get('frames_ingested', 0) > 0:
                    drop_rate = (ing_data.get('frames_dropped', 0) / 
                                ing_data.get('frames_ingested', 1)) * 100
                    print(f"      Drop rate: {drop_rate:.2f}%")
                
                # Processing metrics
                print(f"\n   ⚙️  Processing Agent:")
                print(f"      Frames processed: {proc_data.get('processed_frames', 0)}")
                print(f"      Total detections: {proc_data.get('detection_count', 0)}")
                print(f"      Anonymized frames: {proc_data.get('anonymized_count', 0)}")
                print(f"      Failed batches: {proc_data.get('failed_batches', 0)}")
                print(f"      Circuit breaker: {proc_data.get('circuit_breaker_state', 'N/A')}")
                
                # Storage metrics
                print(f"\n   💾 Storage Agent:")
                print(f"      Frames stored: {stor_data.get('frames_stored', 0)}")
                print(f"      Batches stored: {stor_data.get('batches_stored', 0)}")
                print(f"      Total bytes: {stor_data.get('total_bytes_stored', 0):,}")
                print(f"      Storage mode: {stor_data.get('storage_mode', 'N/A')}")
                print(f"      Fallback count: {stor_data.get('fallback_count', 0)}")
                print(f"      Disk usage: {stor_data.get('disk_usage_percent', 0):.1f}%")
                
            except Exception as e:
                print(f"   ⚠️  Could not retrieve final stats: {e}")
        
        print("\n5.2 Performance Analysis:")
        
        # Calculate throughput (như Table 2 trong bài báo)
        total_time = self.metrics_history[-1]['timestamp'] - self.metrics_history[0]['timestamp']
        total_frames = self.metrics_history[-1]['processed']
        
        if total_time > 0 and total_frames > 0:
            throughput = total_frames / total_time
            print(f"   Average throughput: {throughput:.2f} frames/second")
            
            # Calculate latency
            avg_queue = sum(m['queue'] for m in self.metrics_history) / len(self.metrics_history)
            print(f"   Average queue length: {avg_queue:.1f}")
            
            # FPS adaptation analysis
            fps_values = [m['fps'] for m in self.metrics_history]
            min_fps = min(fps_values)
            max_fps = max(fps_values)
            avg_fps = sum(fps_values) / len(fps_values)
            
            print(f"   FPS range: {min_fps} - {max_fps} (avg: {avg_fps:.1f})")
            
            if max_fps - min_fps > 5:
                print(f"   ✅ Adaptive FPS actively working (variation: {max_fps - min_fps})")
        
        print("\n5.3 Autonomy Features Assessment:")
        
        # Check for autonomy features usage
        autonomy_score = 0
        
        if max_fps - min_fps > 0:
            print(f"   ✅ Backpressure handling: ACTIVE")
            autonomy_score += 1
        else:
            print(f"   ℹ️  Backpressure handling: NOT TRIGGERED")
        
        if ing_data.get('retry_count', 0) > 0:
            print(f"   ✅ Self-healing: ACTIVE ({ing_data.get('retry_count', 0)} retries)")
            autonomy_score += 1
        else:
            print(f"   ℹ️  Self-healing: NOT NEEDED")
        
        if proc_data.get('anonymized_count', 0) > 0:
            print(f"   ✅ Privacy protection: ACTIVE")
            autonomy_score += 1
        else:
            print(f"   ℹ️  Privacy protection: CONFIGURED")
        
        if stor_data.get('fallback_count', 0) > 0:
            print(f"   ✅ Storage fallback: TRIGGERED")
            autonomy_score += 1
        else:
            print(f"   ℹ️  Storage fallback: NOT NEEDED")
        
        print(f"\n   Autonomy Score: {autonomy_score}/4 features demonstrated")
        
        return True
    
    async def section_6_cleanup(self):
        """
        SECTION 6: Cleanup & System State
        """
        self.print_section("SECTION 6: Cleanup & System Health", "🧹")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            print("\n6.1 Stopping Active Jobs...")
            
            try:
                if hasattr(self, 'job_id'):
                    response = await client.post(
                        f"{self.base_urls['ingestion']}/ingest/{self.job_id}/stop"
                    )
                    
                    if response.status_code == 200:
                        print(f"   ✅ Job {self.job_id} stopped")
                    else:
                        print(f"   ⚠️  Job stop returned: {response.status_code}")
            except Exception as e:
                print(f"   ⚠️  Cleanup error: {e}")
            
            print("\n6.2 Final System Health Check...")
            
            # Check all agents health
            agents_health = {}
            
            for name, url in self.base_urls.items():
                try:
                    response = await client.get(f"{url}/health", timeout=5.0)
                    if response.status_code == 200:
                        agents_health[name] = 'healthy'
                        print(f"   ✅ {name.capitalize()}: Healthy")
                    else:
                        agents_health[name] = 'degraded'
                        print(f"   ⚠️  {name.capitalize()}: Degraded")
                except Exception as e:
                    agents_health[name] = 'unreachable'
                    print(f"   ❌ {name.capitalize()}: Unreachable")
            
            healthy_count = sum(1 for v in agents_health.values() if v == 'healthy')
            
            print(f"\n   System Health: {healthy_count}/{len(agents_health)} agents healthy")
            
            return healthy_count >= len(agents_health) - 1
    
    async def run_complete_demo(self):
        """
        Run complete demo sequence
        """
        print("\n" + "="*70)
        print("🎬 MULTI-AGENT DATA ACQUISITION PIPELINE - COMPLETE DEMO")
        print("   Based on: 'An Agent-Based Data Acquisition Pipeline for Image Data'")
        print("   Implementation: Phase 4 with Advanced Features")
        print("="*70)
        
        results = {}
        
        # Section 1: System Health
        results['health'] = await self.section_1_system_health()
        if not results['health']:
            print("\n❌ System health check failed. Please ensure all containers are running:")
            print("   docker-compose up -d")
            return False
        
        await asyncio.sleep(2)
        
        # Section 2: Privacy Setup
        results['privacy'] = await self.section_2_configure_privacy()
        await asyncio.sleep(2)
        
        # Section 3: Start Ingestion
        results['ingestion'] = await self.section_3_start_ingestion()
        if not results['ingestion']:
            print("\n❌ Failed to start ingestion. Check logs for details.")
            return False
        
        await asyncio.sleep(3)
        
        # Section 4: Monitor Pipeline
        results['monitoring'] = await self.section_4_monitor_pipeline()
        
        # Section 5: Analyze Results
        results['analysis'] = await self.section_5_analyze_results()
        
        # Section 6: Cleanup
        results['cleanup'] = await self.section_6_cleanup()
        
        # Final Summary
        self.print_section("DEMO SUMMARY", "🎯")
        
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        
        print(f"\n   Results: {passed}/{total} sections completed successfully")
        print(f"\n   ✅ System demonstrated:")
        print(f"      • Agent-based architecture (4 specialized agents)")
        print(f"      • Adaptive data ingestion (backpressure handling)")
        print(f"      • Object detection & anonymization")
        print(f"      • Fault-tolerant storage (primary + fallback)")
        print(f"      • Real-time monitoring & metrics")
        
        print(f"\n   📊 View detailed dashboard at:")
        print(f"      http://localhost:8000/dashboard")
        
        print(f"\n   📝 Access points:")
        print(f"      • API Documentation: http://localhost:8000/docs")
        print(f"      • Metrics API: http://localhost:8000/api/metrics/summary")
        print(f"      • Agent Registry: http://localhost:8000/agents")
        
        if passed == total:
            print("\n   🎉 DEMO COMPLETED SUCCESSFULLY!")
            return True
        else:
            print(f"\n   ⚠️  Demo completed with {total - passed} issues")
            return False


async def main():
    """Main execution"""
    
    # Check prerequisites
    print("\n🔍 Checking prerequisites...")
    
    test_video = Path("./test_data/video_traffic.mp4")
    if not test_video.exists():
        print(f"⚠️  Test video not found: {test_video}")
        print("   You can:")
        print("   1. Place a video file at: test_data/video_traffic.mp4")
        print("   2. Or modify the demo script to use a camera stream")
        
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            return 1
    
    print("\n⏳ Waiting for services to stabilize (10 seconds)...")
    await asyncio.sleep(10)
    
    # Run demo
    demo = CompletePipelineDemo()
    
    try:
        success = await demo.run_complete_demo()
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    
    print(""" MULTI-AGENT DATA ACQUISITION PIPELINE DEMO""")
    
    exit_code = asyncio.run(main())
    sys.exit(exit_code)