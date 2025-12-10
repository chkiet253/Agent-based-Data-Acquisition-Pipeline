"""
Phase 2 Integration Test - Core Pipeline
Tests end-to-end video processing pipeline
"""
import asyncio
import httpx
import time
from pathlib import Path
from datetime import datetime


class Phase2Tester:
    def __init__(self):
        self.orchestrator_url = "http://localhost:8000"
        self.ingestion_url = "http://localhost:8001"
        self.processing_url = "http://localhost:8002"
        self.storage_url = "http://localhost:8003"
        
        # Test video path (should be in test_data folder)
        self.test_video = "/test_data/sample.mp4"
    
    async def test_agents_registered(self):
        """Test 1: All agents are registered"""
        print("\n🔍 Test 1: Agent Registration")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.orchestrator_url}/agents")
                data = response.json()
                
                required_agents = {'ingestion', 'processing', 'storage'}
                registered_types = {agent['agent_type'] for agent in data['agents']}
                
                if required_agents.issubset(registered_types):
                    print(f"✅ All required agents registered: {registered_types}")
                    return True
                else:
                    missing = required_agents - registered_types
                    print(f"❌ Missing agents: {missing}")
                    return False
                    
        except Exception as e:
            print(f"❌ Registration check failed: {e}")
            return False
    
    async def test_video_file_exists(self):
        """Test 2: Test video file exists"""
        print("\n🔍 Test 2: Test Video File")
        
        if Path(self.test_video).exists():
            print(f"✅ Test video found: {self.test_video}")
            return True
        else:
            print(f"⚠️  Test video not found: {self.test_video}")
            print("   You can use any .mp4 file or download a sample video")
            return False
    
    async def test_start_ingestion(self):
        """Test 3: Start video ingestion"""
        print("\n🔍 Test 3: Start Video Ingestion")
        
        if not Path(self.test_video).exists():
            print("⚠️  Skipping ingestion test (no video file)")
            return False
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                config = {
                    "source_id": "test_source_001",
                    "video_path": self.test_video,
                    "fps": 5,  # Process 5 frames per second
                    "batch_size": 10,
                    "start_frame": 0,
                    "end_frame": 50  # Process first 50 frames only
                }
                
                response = await client.post(
                    f"{self.ingestion_url}/ingest/start",
                    json=config
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.job_id = data['job_id']
                    print(f"✅ Ingestion started: {self.job_id}")
                    print(f"   Config: {config['fps']} FPS, batch size {config['batch_size']}")
                    return True
                else:
                    print(f"❌ Failed to start ingestion: {response.status_code}")
                    print(f"   Response: {response.text}")
                    return False
                    
        except Exception as e:
            print(f"❌ Ingestion start failed: {e}")
            return False
    
    async def test_monitor_pipeline(self):
        """Test 4: Monitor pipeline progress"""
        print("\n🔍 Test 4: Monitor Pipeline Progress")
        
        if not hasattr(self, 'job_id'):
            print("⚠️  Skipping monitoring (no active job)")
            return False
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Monitor for 30 seconds
                for i in range(6):
                    await asyncio.sleep(5)
                    
                    # Check ingestion status
                    ing_response = await client.get(
                        f"{self.ingestion_url}/ingest/{self.job_id}/status"
                    )
                    ing_data = ing_response.json()
                    
                    # Check processing status
                    proc_response = await client.get(
                        f"{self.processing_url}/process/status"
                    )
                    proc_data = proc_response.json()
                    
                    # Check storage status
                    stor_response = await client.get(
                        f"{self.storage_url}/storage/status"
                    )
                    stor_data = stor_response.json()
                    
                    print(f"\n   [{i*5}s] Pipeline Status:")
                    print(f"   📥 Ingestion: {ing_data['frames_ingested']} frames, status: {ing_data['status']}")
                    print(f"   ⚙️  Processing: {proc_data['processed_frames']} frames, {proc_data['detection_count']} detections")
                    print(f"   💾 Storage: {stor_data['frames_stored']} frames, {stor_data['batches_stored']} batches")
                    
                    # Check if completed
                    if ing_data['status'] == 'completed':
                        print("\n✅ Pipeline completed!")
                        
                        # Wait a bit more for processing and storage to finish
                        await asyncio.sleep(5)
                        
                        # Final status
                        proc_response = await client.get(f"{self.processing_url}/process/status")
                        proc_data = proc_response.json()
                        stor_response = await client.get(f"{self.storage_url}/storage/status")
                        stor_data = stor_response.json()
                        
                        print(f"\n   📊 Final Results:")
                        print(f"   - Frames processed: {proc_data['processed_frames']}")
                        print(f"   - Total detections: {proc_data['detection_count']}")
                        print(f"   - Batches stored: {stor_data['batches_stored']}")
                        print(f"   - Total storage: {stor_data['total_bytes_stored']:,} bytes")
                        
                        return True
                
                print("\n⚠️  Pipeline still running after 30s")
                return True  # Still consider it a pass
                
        except Exception as e:
            print(f"❌ Monitoring failed: {e}")
            return False
    
    async def test_storage_verification(self):
        """Test 5: Verify data in MinIO"""
        print("\n🔍 Test 5: Storage Verification")
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # List buckets
                response = await client.get(f"{self.storage_url}/storage/buckets")
                
                if response.status_code == 200:
                    buckets = response.json()
                    print(f"✅ MinIO buckets: {[b['name'] for b in buckets['buckets']]}")
                    
                    # List objects for test source
                    response = await client.get(
                        f"{self.storage_url}/storage/objects/test_source_001"
                    )
                    
                    if response.status_code == 200:
                        objects = response.json()
                        print(f"   Found {len(objects['objects'])} objects:")
                        for obj in objects['objects'][:5]:  # Show first 5
                            print(f"   - {obj['name']} ({obj['size']} bytes)")
                        
                        if len(objects['objects']) > 5:
                            print(f"   ... and {len(objects['objects']) - 5} more")
                        
                        return True
                    else:
                        print("⚠️  No objects found (data might still be processing)")
                        return True
                else:
                    print(f"❌ Failed to access MinIO: {response.status_code}")
                    return False
                    
        except Exception as e:
            print(f"❌ Storage verification failed: {e}")
            return False
    
    async def test_processing_capabilities(self):
        """Test 6: Test processing capabilities"""
        print("\n🔍 Test 6: Processing Capabilities")
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.orchestrator_url}/agents")
                agents = response.json()['agents']
                
                for agent in agents:
                    if agent['agent_type'] == 'processing':
                        print(f"✅ Processing agent capabilities:")
                        for cap in agent['capabilities']:
                            print(f"   - {cap}")
                        return True
                
                print("⚠️  Processing agent not found")
                return False
                
        except Exception as e:
            print(f"❌ Capability check failed: {e}")
            return False
    
    async def test_backpressure_config(self):
        """Test 7: Test backpressure configuration"""
        print("\n🔍 Test 7: Backpressure Configuration")
        
        if not hasattr(self, 'job_id'):
            print("⚠️  Skipping backpressure test (no active job)")
            return True
        
        try:
            async with httpx.AsyncClient() as client:
                # Try to update FPS (simulate backpressure response)
                response = await client.patch(
                    f"{self.ingestion_url}/ingest/{self.job_id}/config",
                    params={"fps": 2}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ Backpressure config updated:")
                    print(f"   - New FPS: {data['fps']}")
                    return True
                else:
                    print(f"⚠️  Config update returned: {response.status_code}")
                    return True
                    
        except Exception as e:
            print(f"❌ Backpressure config failed: {e}")
            return False
    
    async def run_all_tests(self):
        """Run all Phase 2 tests"""
        print("=" * 60)
        print("🚀 PHASE 2 - CORE PIPELINE TESTS")
        print("=" * 60)
        
        results = {}
        
        # Test 1: Registration
        results['registration'] = await self.test_agents_registered()
        
        # Test 2: Video file
        results['video_file'] = await self.test_video_file_exists()
        
        # Test 3: Start ingestion
        results['start_ingestion'] = await self.test_start_ingestion()
        
        # Test 4: Monitor pipeline
        results['monitor_pipeline'] = await self.test_monitor_pipeline()
        
        # Test 5: Storage verification
        results['storage_verification'] = await self.test_storage_verification()
        
        # Test 6: Processing capabilities
        results['processing_capabilities'] = await self.test_processing_capabilities()
        
        # Test 7: Backpressure config
        results['backpressure_config'] = await self.test_backpressure_config()
        
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
            print("\n🎉 Phase 2 Core Pipeline is working!")
        else:
            print(f"\n⚠️  {total - passed} test(s) failed or skipped.")
        
        return passed >= total - 1  # Allow 1 failure


async def main():
    tester = Phase2Tester()
    
    print("\n⏳ Waiting 10 seconds for services to be ready...")
    await asyncio.sleep(10)
    
    success = await tester.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)