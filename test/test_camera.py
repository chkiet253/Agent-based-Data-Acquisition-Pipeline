"""
Quick test với Insecam camera - READY TO RUN
Camera: https://www.insecam.org/en/view/1010039/ (Japan)
"""
import asyncio
import httpx
import cv2
import time
from datetime import datetime


class InsecamTester:
    def __init__(self):
        self.base_url = "http://localhost:8001"
        
        # Insecam camera từ screenshot
        self.camera_id = "1010039"
        self.camera_url = f"https://www.insecam.org/en/view/{self.camera_id}/"
        
        # Stream URL sẽ được extract tự động
        self.stream_url = None
    
    def extract_stream_url(self):
        """
        Extract stream URL từ Insecam page
        """
        print(f"🔍 Extracting stream URL from: {self.camera_url}")
        
        try:
            import requests
            import re
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(self.camera_url, headers=headers, timeout=10)
            
            # Tìm image URL trong HTML
            pattern = r'<img[^>]+id="image0"[^>]+src="([^"]+)"'
            match = re.search(pattern, response.text)
            
            if match:
                self.stream_url = match.group(1)
                print(f"✅ Found stream URL: {self.stream_url}")
                return True
            else:
                print("❌ Could not extract stream URL")
                
                # Fallback: Dùng URL mẫu
                print("\n⚠️  Using fallback stream URL")
                print("   You may need to manually get the correct URL")
                
                # Example fallback URLs để test
                fallback_urls = [
                    "http://60.32.174.146/image.jpg",  # Example IP
                    "http://60.32.174.146/axis-cgi/mjpg/video.cgi",
                ]
                
                print("\nTry these URLs manually:")
                for i, url in enumerate(fallback_urls, 1):
                    print(f"  {i}. {url}")
                
                return False
                
        except Exception as e:
            print(f"❌ Error extracting URL: {e}")
            return False
    
    def test_connection(self):
        """
        Test connection to camera
        """
        if not self.stream_url:
            print("❌ No stream URL available")
            return False
        
        print(f"\n🧪 Testing connection to: {self.stream_url}")
        
        try:
            cap = cv2.VideoCapture(self.stream_url)
            
            if not cap.isOpened():
                print("❌ Cannot open stream")
                return False
            
            # Try to read a frame
            ret, frame = cap.read()
            
            if ret and frame is not None:
                print(f"✅ Connection successful!")
                print(f"   Resolution: {frame.shape[1]}x{frame.shape[0]}")
                
                # Save test frame
                cv2.imwrite("insecam_test_frame.jpg", frame)
                print(f"   Saved test frame: insecam_test_frame.jpg")
                
                cap.release()
                return True
            else:
                print("❌ Cannot read frame")
                cap.release()
                return False
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    async def run_pipeline_test(self, duration_seconds: int = 30):
        """
        Test với pipeline hoàn chỉnh
        """
        print(f"\n🚀 Starting pipeline test")
        print(f"   Duration: {duration_seconds}s")
        print(f"   Stream: {self.stream_url}")
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                # Config cho Insecam camera
                config = {
                    "source_id": f"insecam_{self.camera_id}",
                    "video_path": self.stream_url,
                    "fps": 3,  # Low FPS cho snapshot-based stream
                    "batch_size": 5,
                    "start_frame": 0,
                    "end_frame": duration_seconds * 3,
                    "adaptive_mode": True,
                    "min_fps": 1,
                    "max_fps": 5
                }
                
                # Start ingestion
                print("\n📥 Starting ingestion...")
                response = await client.post(
                    f"{self.base_url}/ingest/start",
                    json=config
                )
                
                if response.status_code != 200:
                    print(f"❌ Failed to start: {response.status_code}")
                    print(f"   Response: {response.text}")
                    return False
                
                job_id = response.json()['job_id']
                print(f"✅ Job started: {job_id}")
                
                # Monitor progress
                print(f"\n📊 Monitoring (updating every 3s)...\n")
                
                start_time = time.time()
                
                while time.time() - start_time < duration_seconds:
                    await asyncio.sleep(3)
                    
                    # Get status
                    status_resp = await client.get(
                        f"{self.base_url}/ingest/{job_id}/status"
                    )
                    
                    if status_resp.status_code == 200:
                        status = status_resp.json()
                        
                        elapsed = int(time.time() - start_time)
                        
                        print(f"[{elapsed:02d}s] "
                              f"Ingested: {status['frames_ingested']:3d} | "
                              f"Dropped: {status['frames_dropped']:2d} | "
                              f"FPS: {status['current_fps']:2d} | "
                              f"Status: {status['status']}")
                        
                        if status['status'] in ['completed', 'failed']:
                            break
                
                # Stop job
                await client.post(f"{self.base_url}/ingest/{job_id}/stop")
                
                # Final stats
                print("\n" + "="*60)
                print("📊 FINAL RESULTS")
                print("="*60)
                
                final_resp = await client.get(f"{self.base_url}/ingest/{job_id}/status")
                
                if final_resp.status_code == 200:
                    final = final_resp.json()
                    
                    print(f"Total frames ingested: {final['frames_ingested']}")
                    print(f"Total frames dropped: {final['frames_dropped']}")
                    print(f"Final status: {final['status']}")
                    
                    # Processing stats
                    proc_resp = await client.get("http://localhost:8002/process/status")
                    if proc_resp.status_code == 200:
                        proc = proc_resp.json()
                        print(f"\nProcessing:")
                        print(f"  Processed frames: {proc['processed_frames']}")
                        print(f"  Detections: {proc['detection_count']}")
                    
                    # Storage stats
                    stor_resp = await client.get("http://localhost:8003/storage/status")
                    if stor_resp.status_code == 200:
                        stor = stor_resp.json()
                        print(f"\nStorage:")
                        print(f"  Stored frames: {stor['frames_stored']}")
                        print(f"  Storage mode: {stor['storage_mode']}")
                
                print("="*60)
                
                return True
                
        except Exception as e:
            print(f"❌ Pipeline test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def run_with_privacy(self, duration_seconds: int = 20):
        """
        Test với privacy features
        """
        print(f"\n🔒 Testing with privacy features")
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                # Add privacy zones
                print("\n1. Adding privacy zones...")
                
                # Zone để che biển số xe
                await client.post(
                    "http://localhost:8002/process/privacy/zone",
                    params={"x": 200, "y": 400, "width": 300, "height": 100}
                )
                print("   ✅ License plate zone added")
                
                # Config with anonymization
                config = {
                    "source_id": f"insecam_{self.camera_id}_privacy",
                    "video_path": self.stream_url,
                    "fps": 2,
                    "batch_size": 5,
                    "end_frame": duration_seconds * 2,
                    "adaptive_mode": True
                }
                
                # Start
                response = await client.post(
                    f"{self.base_url}/ingest/start",
                    json=config
                )
                
                if response.status_code != 200:
                    print(f"❌ Failed to start")
                    return False
                
                job_id = response.json()['job_id']
                print(f"\n✅ Privacy-enabled stream started: {job_id}")
                
                # Monitor with privacy stats
                for i in range(duration_seconds // 3):
                    await asyncio.sleep(3)
                    
                    privacy_resp = await client.get(
                        "http://localhost:8002/process/privacy/stats"
                    )
                    
                    if privacy_resp.status_code == 200:
                        stats = privacy_resp.json()
                        print(f"\n[{i*3}s] Privacy Stats:")
                        print(f"   Total operations: {stats.get('total_operations', 0)}")
                        print(f"   Faces detected: {stats.get('total_faces', 0)}")
                        print(f"   Plates detected: {stats.get('total_plates', 0)}")
                
                # Stop
                await client.post(f"{self.base_url}/ingest/{job_id}/stop")
                
                return True
                
        except Exception as e:
            print(f"❌ Privacy test failed: {e}")
            return False


async def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║         INSECAM CAMERA TESTER                            ║
║  Camera: Japan Traffic Cam (ID: 1010039)                ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    tester = InsecamTester()
    
    # Step 1: Extract stream URL
    print("\n" + "="*60)
    print("STEP 1: Extract Stream URL")
    print("="*60)
    
    if not tester.extract_stream_url():
        print("\n⚠️  Could not extract stream URL automatically")
        print("\nManual steps:")
        print("1. Open https://www.insecam.org/en/view/1010039/")
        print("2. Right-click on camera image → Inspect")
        print("3. Find <img id='image0' src='...'> ")
        print("4. Copy the src URL")
        print("5. Update stream_url in code")
        return 1
    
    # Step 2: Test connection
    print("\n" + "="*60)
    print("STEP 2: Test Connection")
    print("="*60)
    
    if not tester.test_connection():
        print("\n❌ Connection test failed")
        print("   Camera may be offline or URL is incorrect")
        return 1
    
    # Step 3: Wait for services
    print("\n" + "="*60)
    print("STEP 3: Check Pipeline Services")
    print("="*60)
    
    print("\nWaiting for services to be ready...")
    await asyncio.sleep(5)
    
    # Check services
    async with httpx.AsyncClient(timeout=10.0) as client:
        services = [
            ("Orchestrator", "http://localhost:8000/health"),
            ("Ingestion", "http://localhost:8001/health"),
            ("Processing", "http://localhost:8002/health"),
            ("Storage", "http://localhost:8003/health")
        ]
        
        all_ready = True
        for name, url in services:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    print(f"✅ {name}: Ready")
                else:
                    print(f"❌ {name}: Not ready ({resp.status_code})")
                    all_ready = False
            except Exception as e:
                print(f"❌ {name}: Not reachable")
                all_ready = False
        
        if not all_ready:
            print("\n⚠️  Some services are not ready")
            print("   Run: docker-compose up -d")
            return 1
    
    # Step 4: Run pipeline test
    print("\n" + "="*60)
    print("STEP 4: Pipeline Test (30s)")
    print("="*60)
    
    success = await tester.run_pipeline_test(duration_seconds=30)
    
    if not success:
        print("\n❌ Pipeline test failed")
        return 1
    
    # Step 5: Privacy test
    print("\n" + "="*60)
    print("STEP 5: Privacy Features Test (20s)")
    print("="*60)
    
    await tester.run_with_privacy(duration_seconds=20)
    
    # Summary
    print("\n" + "="*60)
    print("✅ ALL TESTS COMPLETED")
    print("="*60)
    
    print("\n📊 View results:")
    print("   Dashboard: http://localhost:8000/dashboard")
    print("   Metrics: http://localhost:8000/api/metrics/summary")
    
    return 0


if __name__ == "__main__":
    import sys
    
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)