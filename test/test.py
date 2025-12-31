"""
Test với camera Nhật Bản tìm được
IP: 220.254.72.200
Port: 80
Path: /cgi-bin/camera

READY TO RUN - Chạy ngay không cần sửa gì
"""
import asyncio
import httpx
import cv2
import time
from datetime import datetime


# ========== CAMERA URL TÌM ĐƯỢC ==========
CAMERA_URL = "http://220.254.72.200/cgi-bin/camera?resolution=640&quality=1"
CAMERA_IP = "220.254.72.200"
# ==========================================


def test_camera_connection():
    """
    Test 1: Kiểm tra có kết nối được camera không
    """
    print("="*60)
    print("🔍 TEST 1: Camera Connection")
    print("="*60)
    print(f"Camera IP: {CAMERA_IP}")
    print(f"Camera URL: {CAMERA_URL}")
    print()
    
    try:
        print("⏳ Connecting to camera...")
        cap = cv2.VideoCapture(CAMERA_URL)
        
        if not cap.isOpened():
            print("❌ Cannot open camera stream")
            print("\nTrying alternative URLs...")
            
            # Try alternatives
            alternatives = [
                f"http://{CAMERA_IP}/cgi-bin/camera",
                f"http://{CAMERA_IP}/image.jpg",
                f"http://{CAMERA_IP}/snapshot.cgi",
            ]
            
            for alt_url in alternatives:
                print(f"\n   Trying: {alt_url}")
                cap = cv2.VideoCapture(alt_url)
                if cap.isOpened():
                    print(f"   ✅ This URL works!")
                    global CAMERA_URL
                    CAMERA_URL = alt_url
                    break
                cap.release()
            else:
                return False
        
        # Read test frames
        print("\n📸 Reading test frames...")
        success_count = 0
        
        for i in range(5):
            ret, frame = cap.read()
            
            if ret and frame is not None:
                success_count += 1
                print(f"   Frame {i+1}/5: ✅ {frame.shape[1]}x{frame.shape[0]}")
                
                if i == 0:
                    # Save first frame
                    cv2.imwrite("camera_test_frame.jpg", frame)
                    print(f"   💾 Saved: camera_test_frame.jpg")
            else:
                print(f"   Frame {i+1}/5: ❌ Failed")
            
            time.sleep(0.5)
        
        cap.release()
        
        print(f"\n📊 Result: {success_count}/5 frames successful")
        
        if success_count >= 3:
            print("✅ Camera connection: GOOD")
            return True
        else:
            print("⚠️  Camera connection: UNSTABLE")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


async def test_pipeline_basic(duration_seconds=30):
    """
    Test 2: Test với pipeline (basic)
    """
    print("\n" + "="*60)
    print("🚀 TEST 2: Pipeline Basic Test")
    print("="*60)
    print(f"Duration: {duration_seconds} seconds")
    print(f"Camera: {CAMERA_URL}")
    print()
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            
            # Check services first
            print("🔍 Checking services...")
            services = {
                "Orchestrator": "http://localhost:8000/health",
                "Ingestion": "http://localhost:8001/health",
                "Processing": "http://localhost:8002/health",
                "Storage": "http://localhost:8003/health"
            }
            
            all_ready = True
            for name, url in services.items():
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        print(f"   ✅ {name}")
                    else:
                        print(f"   ❌ {name} (status: {resp.status_code})")
                        all_ready = False
                except Exception as e:
                    print(f"   ❌ {name} (not reachable)")
                    all_ready = False
            
            if not all_ready:
                print("\n⚠️  Some services not ready!")
                print("   Run: docker-compose up -d")
                return False
            
            # Start ingestion
            print("\n📥 Starting ingestion...")
            
            config = {
                "source_id": "japan_camera_220_254_72_200",
                "video_path": CAMERA_URL,
                "fps": 3,  # Low FPS cho stability
                "batch_size": 5,
                "start_frame": 0,
                "end_frame": duration_seconds * 3,  # 3 fps * duration
                "adaptive_mode": True,
                "min_fps": 1,
                "max_fps": 5
            }
            
            response = await client.post(
                "http://localhost:8001/ingest/start",
                json=config
            )
            
            if response.status_code != 200:
                print(f"❌ Failed to start ingestion")
                print(f"   Status: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
            
            job_data = response.json()
            job_id = job_data['job_id']
            
            print(f"✅ Ingestion started: {job_id}")
            print(f"\n📊 Monitoring for {duration_seconds} seconds...")
            print("   (Updates every 3 seconds)\n")
            
            # Monitor
            start_time = time.time()
            last_frames = 0
            
            while time.time() - start_time < duration_seconds:
                await asyncio.sleep(3)
                
                # Get status
                ing_resp = await client.get(
                    f"http://localhost:8001/ingest/{job_id}/status"
                )
                proc_resp = await client.get(
                    "http://localhost:8002/process/status"
                )
                stor_resp = await client.get(
                    "http://localhost:8003/storage/status"
                )
                
                if ing_resp.status_code == 200:
                    elapsed = int(time.time() - start_time)
                    
                    ing_data = ing_resp.json()
                    proc_data = proc_resp.json() if proc_resp.status_code == 200 else {}
                    stor_data = stor_resp.json() if stor_resp.status_code == 200 else {}
                    
                    frames = ing_data.get('frames_ingested', 0)
                    fps_current = ing_data.get('current_fps', 0)
                    status = ing_data.get('status', 'unknown')
                    
                    frames_rate = (frames - last_frames) / 3 if elapsed > 0 else 0
                    last_frames = frames
                    
                    print(f"[{elapsed:02d}s] "
                          f"📥 Ingested: {frames:3d} ({frames_rate:.1f} fps) | "
                          f"⚙️  Processed: {proc_data.get('processed_frames', 0):3d} | "
                          f"💾 Stored: {stor_data.get('frames_stored', 0):3d} | "
                          f"📦 Queue: {proc_data.get('queue_length', 0):2d} | "
                          f"Status: {status}")
                    
                    if status in ['completed', 'failed']:
                        break
            
            # Stop
            print("\n⏹️  Stopping ingestion...")
            await client.post(f"http://localhost:8001/ingest/{job_id}/stop")
            
            # Final stats
            print("\n" + "="*60)
            print("📊 FINAL RESULTS")
            print("="*60)
            
            final_ing = await client.get(f"http://localhost:8001/ingest/{job_id}/status")
            final_proc = await client.get("http://localhost:8002/process/status")
            final_stor = await client.get("http://localhost:8003/storage/status")
            
            if final_ing.status_code == 200:
                ing = final_ing.json()
                proc = final_proc.json() if final_proc.status_code == 200 else {}
                stor = final_stor.json() if final_stor.status_code == 200 else {}
                
                print(f"\n📥 Ingestion:")
                print(f"   Total frames: {ing.get('frames_ingested', 0)}")
                print(f"   Dropped frames: {ing.get('frames_dropped', 0)}")
                print(f"   Final status: {ing.get('status', 'unknown')}")
                
                print(f"\n⚙️  Processing:")
                print(f"   Processed frames: {proc.get('processed_frames', 0)}")
                print(f"   Detections: {proc.get('detection_count', 0)}")
                print(f"   Circuit breaker: {proc.get('circuit_breaker_state', 'N/A')}")
                
                print(f"\n💾 Storage:")
                print(f"   Stored frames: {stor.get('frames_stored', 0)}")
                print(f"   Stored batches: {stor.get('batches_stored', 0)}")
                print(f"   Storage mode: {stor.get('storage_mode', 'N/A')}")
                
                # Calculate stats
                total_frames = ing.get('frames_ingested', 0)
                if total_frames > 0:
                    avg_fps = total_frames / duration_seconds
                    success_rate = (proc.get('processed_frames', 0) / total_frames) * 100
                    
                    print(f"\n📈 Statistics:")
                    print(f"   Average FPS: {avg_fps:.2f}")
                    print(f"   Success rate: {success_rate:.1f}%")
            
            print("\n" + "="*60)
            
            return True
            
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_with_privacy(duration_seconds=20):
    """
    Test 3: Test với privacy features
    """
    print("\n" + "="*60)
    print("🔒 TEST 3: Privacy Features Test")
    print("="*60)
    print(f"Duration: {duration_seconds} seconds")
    print()
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            
            # Add privacy zones
            print("🔧 Configuring privacy zones...")
            
            zones = [
                (100, 200, 400, 150, "License plates area"),
                (500, 100, 200, 200, "Face privacy zone"),
            ]
            
            for x, y, w, h, desc in zones:
                resp = await client.post(
                    "http://localhost:8002/process/privacy/zone",
                    params={"x": x, "y": y, "width": w, "height": h}
                )
                
                if resp.status_code == 200:
                    print(f"   ✅ {desc}: ({x}, {y}) {w}x{h}")
                else:
                    print(f"   ⚠️  Failed to add zone: {desc}")
            
            # Get initial privacy stats
            stats_resp = await client.get(
                "http://localhost:8002/process/privacy/stats"
            )
            
            if stats_resp.status_code == 200:
                print("\n📊 Privacy stats (before):")
                stats = stats_resp.json()
                for key, value in stats.items():
                    print(f"   {key}: {value}")
            
            # Start ingestion with privacy
            print("\n📥 Starting privacy-enabled ingestion...")
            
            config = {
                "source_id": "japan_camera_privacy",
                "video_path": CAMERA_URL,
                "fps": 2,
                "batch_size": 5,
                "end_frame": duration_seconds * 2,
                "adaptive_mode": True
            }
            
            response = await client.post(
                "http://localhost:8001/ingest/start",
                json=config
            )
            
            if response.status_code != 200:
                print("❌ Failed to start")
                return False
            
            job_id = response.json()['job_id']
            print(f"✅ Job started: {job_id}")
            
            # Monitor
            print(f"\n📊 Monitoring...\n")
            
            for i in range(duration_seconds // 3):
                await asyncio.sleep(3)
                
                # Get privacy stats
                stats_resp = await client.get(
                    "http://localhost:8002/process/privacy/stats"
                )
                
                if stats_resp.status_code == 200:
                    stats = stats_resp.json()
                    
                    print(f"[{i*3:02d}s] "
                          f"🔒 Anonymizations: {stats.get('total_operations', 0)} | "
                          f"👤 Faces: {stats.get('total_faces', 0)} | "
                          f"🚗 Plates: {stats.get('total_plates', 0)} | "
                          f"📍 Zones: {stats.get('total_zones', 0)}")
            
            # Stop
            await client.post(f"http://localhost:8001/ingest/{job_id}/stop")
            
            print("\n✅ Privacy test completed")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Privacy test failed: {e}")
        return False


async def main():
    """
    Main test runner
    """
    print("""
╔══════════════════════════════════════════════════════════╗
║         JAPAN CAMERA TEST                                ║
║  Camera: 220.254.72.200                                  ║
║  Location: Japan (Insecam #1010039)                      ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    results = {}
    
    # Test 1: Connection
    print("\n" + "🎬 Starting tests...\n")
    results['connection'] = test_camera_connection()
    
    if not results['connection']:
        print("\n❌ Camera connection failed. Cannot proceed.")
        print("\nTroubleshooting:")
        print("1. Camera may be offline")
        print("2. IP may have changed")
        print("3. Try another camera from Insecam")
        return 1
    
    # Wait a bit
    print("\n⏳ Waiting 5 seconds before pipeline tests...")
    await asyncio.sleep(5)
    
    # Test 2: Pipeline
    results['pipeline'] = await test_pipeline_basic(duration_seconds=30)
    
    # Test 3: Privacy
    if results['pipeline']:
        await asyncio.sleep(3)
        results['privacy'] = await test_with_privacy(duration_seconds=20)
    else:
        results['privacy'] = False
        print("\n⚠️  Skipping privacy test (pipeline failed)")
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    total = len(results)
    passed_count = sum(1 for v in results.values() if v)
    
    print(f"\n🎯 Results: {passed_count}/{total} tests passed")
    
    if passed_count == total:
        print("\n🎉 ALL TESTS PASSED!")
    else:
        print(f"\n⚠️  {total - passed_count} test(s) failed")
    
    print("\n📊 View results:")
    print("   Dashboard: http://localhost:8000/dashboard")
    print("   Metrics API: http://localhost:8000/api/metrics/summary")
    print("   Test frame: camera_test_frame.jpg")
    
    return 0 if passed_count == total else 1


if __name__ == "__main__":
    import sys
    
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)