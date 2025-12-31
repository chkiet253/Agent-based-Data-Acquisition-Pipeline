"""
Snapshot Camera Stream Test
Đặc biệt cho camera snapshot-based (không phải video stream liên tục)
"""
import asyncio
import httpx
import requests
import time
from datetime import datetime
from pathlib import Path
import io
from PIL import Image
import base64


class SnapshotCameraStreamer:
    """
    Stream từ snapshot camera bằng cách request liên tục
    Tốt hơn cho camera không hỗ trợ video stream
    """
    
    def __init__(self):
        self.base_urls = {
            'orchestrator': 'http://localhost:8000',
            'ingestion': 'http://localhost:8001',
            'processing': 'http://localhost:8002',
            'storage': 'http://localhost:8003'
        }
        
        # Camera snapshot URL
        self.camera_url = "http://220.254.72.200/cgi-bin/camera"
        
        # Request settings
        self.request_timeout = 5
        self.retry_delay = 1
        self.max_retries = 3
    
    def test_snapshot(self) -> bool:
        """Test xem có lấy được snapshot không"""
        print(f"🔍 Testing snapshot: {self.camera_url}")
        
        try:
            response = requests.get(
                self.camera_url,
                timeout=self.request_timeout
            )
            
            if response.status_code == 200:
                # Try to open as image
                img = Image.open(io.BytesIO(response.content))
                print(f"   ✅ Success: {img.size[0]}x{img.size[1]}")
                return True
            else:
                print(f"   ❌ HTTP {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    async def check_services(self) -> bool:
        """Kiểm tra pipeline services"""
        print("\n🔍 Checking services...")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            all_ok = True
            
            for name, url in self.base_urls.items():
                try:
                    response = await client.get(f"{url}/health")
                    if response.status_code == 200:
                        print(f"   ✅ {name.capitalize()}")
                    else:
                        print(f"   ❌ {name.capitalize()}")
                        all_ok = False
                except:
                    print(f"   ❌ {name.capitalize()}")
                    all_ok = False
            
            return all_ok
    
    async def stream_snapshots(self, duration_minutes: int = None, target_fps: float = 1.0):
        """
        Stream snapshot liên tục vào pipeline
        
        Args:
            duration_minutes: Thời gian chạy (phút), None = chạy mãi
            target_fps: FPS mục tiêu (khuyến nghị 0.5-2 cho snapshot camera)
        """
        print(f"\n{'='*60}")
        print("📸 SNAPSHOT CAMERA STREAMING")
        print(f"{'='*60}")
        print(f"Camera: {self.camera_url}")
        if duration_minutes is None:
            print(f"Duration: ♾️  CONTINUOUS (press Ctrl+C to stop)")
        else:
            print(f"Duration: {duration_minutes} minutes")
        print(f"Target rate: {target_fps} snapshots/second")
        print(f"Interval: {1/target_fps:.1f}s between snapshots")
        print()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                
                # Start monitoring job
                print("📥 Starting snapshot collection job...")
                
                start_time = time.time()
                frame_count = 0
                success_count = 0
                error_count = 0
                
                print(f"\n{'='*60}")
                print("📊 LIVE MONITORING (Ctrl+C to stop)")
                print(f"{'='*60}\n")
                
                while True:
                    elapsed = time.time() - start_time
                    
                    # Check duration (chỉ khi có duration_minutes)
                    if duration_minutes is not None and elapsed > duration_minutes * 60:
                        print(f"\n⏱️  Reached {duration_minutes} minutes")
                        break
                    
                    # Get snapshot
                    snapshot_start = time.time()
                    
                    try:
                        # Request snapshot
                        response = requests.get(
                            self.camera_url,
                            timeout=self.request_timeout
                        )
                        
                        if response.status_code == 200:
                            # Convert to base64
                            img_data = base64.b64encode(response.content).decode('utf-8')
                            
                            # Create frame data
                            frame_data = {
                                "frame_id": f"snapshot_{frame_count:06d}",
                                "sequence_number": frame_count,
                                "timestamp": datetime.utcnow().isoformat(),
                                "data": img_data,
                                "metadata": {
                                    "width": 640,
                                    "height": 480,
                                    "format": "jpeg",
                                    "size_bytes": len(response.content)
                                }
                            }
                            
                            # Send to processing (batch of 1)
                            batch = {
                                "batch_id": f"snapshot_batch_{frame_count:06d}",
                                "source_id": "snapshot_camera",
                                "frames": [frame_data]
                            }
                            
                            # Send to processing agent
                            proc_response = await client.post(
                                f"{self.base_urls['processing']}/process/batch",
                                json=batch,
                                timeout=10.0
                            )
                            
                            if proc_response.status_code == 200:
                                success_count += 1
                                
                                # Print update every 5 snapshots
                                if frame_count % 5 == 0:
                                    # Get current stats
                                    proc_status = await client.get(
                                        f"{self.base_urls['processing']}/process/status"
                                    )
                                    stor_status = await client.get(
                                        f"{self.base_urls['storage']}/storage/status"
                                    )
                                    
                                    proc_data = proc_status.json() if proc_status.status_code == 200 else {}
                                    stor_data = stor_status.json() if stor_status.status_code == 200 else {}
                                    
                                    elapsed_min = int(elapsed // 60)
                                    elapsed_sec = int(elapsed % 60)
                                    
                                    print(f"[{elapsed_min:02d}:{elapsed_sec:02d}] "
                                          f"📸 Captured: {success_count:3d} | "
                                          f"❌ Errors: {error_count:2d} | "
                                          f"⚙️  Processed: {proc_data.get('processed_frames', 0):3d} | "
                                          f"🔍 Detections: {proc_data.get('detection_count', 0):3d} | "
                                          f"💾 Stored: {stor_data.get('frames_stored', 0):3d}")
                            else:
                                error_count += 1
                                if error_count % 5 == 0:
                                    print(f"   ⚠️  Processing errors: {error_count}")
                        else:
                            error_count += 1
                            
                    except Exception as e:
                        error_count += 1
                        if error_count % 10 == 0:
                            print(f"   ⚠️  Snapshot errors: {error_count} ({str(e)[:50]})")
                    
                    frame_count += 1
                    
                    # Calculate sleep time to maintain target FPS
                    snapshot_time = time.time() - snapshot_start
                    target_interval = 1.0 / target_fps
                    sleep_time = max(0, target_interval - snapshot_time)
                    
                    await asyncio.sleep(sleep_time)
                
                # Final summary
                total_time = time.time() - start_time
                
                print(f"\n{'='*60}")
                print("📊 FINAL STATISTICS")
                print(f"{'='*60}")
                
                # Get final stats
                proc_resp = await client.get(f"{self.base_urls['processing']}/process/status")
                stor_resp = await client.get(f"{self.base_urls['storage']}/storage/status")
                
                proc_data = proc_resp.json() if proc_resp.status_code == 200 else {}
                stor_data = stor_resp.json() if stor_resp.status_code == 200 else {}
                
                print(f"\n⏱️  Total Time: {int(total_time // 60)}m {int(total_time % 60)}s")
                print(f"\n📸 Snapshot Collection:")
                print(f"   Total attempts: {frame_count}")
                print(f"   Successful: {success_count}")
                print(f"   Errors: {error_count}")
                print(f"   Success rate: {(success_count/frame_count*100):.1f}%")
                print(f"   Average rate: {success_count/total_time:.2f} snapshots/second")
                
                print(f"\n⚙️  Processing:")
                print(f"   Processed frames: {proc_data.get('processed_frames', 0)}")
                print(f"   Total detections: {proc_data.get('detection_count', 0)}")
                print(f"   Queue length: {proc_data.get('queue_length', 0)}")
                
                print(f"\n💾 Storage:")
                print(f"   Stored frames: {stor_data.get('frames_stored', 0)}")
                print(f"   Stored batches: {stor_data.get('batches_stored', 0)}")
                print(f"   Storage mode: {stor_data.get('storage_mode', 'N/A')}")
                
                print(f"\n{'='*60}")
                
                return success_count > 0
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Stream stopped by user")
            return True
            
        except Exception as e:
            print(f"\n❌ Stream failed: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Main function"""
    print("""
╔══════════════════════════════════════════════════════════╗
║         SNAPSHOT CAMERA STREAMER                         ║
║  For cameras that only provide snapshots (not streams)   ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    streamer = SnapshotCameraStreamer()
    
    # Test snapshot
    print("\n" + "="*60)
    print("TEST 1: Snapshot Availability")
    print("="*60)
    
    if not streamer.test_snapshot():
        print("\n❌ Cannot get snapshot from camera")
        print("\nPossible reasons:")
        print("1. Camera is offline")
        print("2. Network connection issue")
        print("3. Camera requires authentication")
        print("\nFallback: Use local video file")
        print("   Place video in: test_data/video_traffic.mp4")
        print("   Run: python test/test_live_stream.py")
        return 1
    
    # Check services
    print("\n" + "="*60)
    print("TEST 2: Pipeline Services")
    print("="*60)
    
    if not await streamer.check_services():
        print("\n❌ Services not ready")
        print("   Run: docker-compose up -d")
        return 1
    
    # Configure streaming
    print("\n" + "="*60)
    print("CONFIGURATION")
    print("="*60)
    
    mode_input = input("Mode (1=Timed, 2=Continuous, default=1): ").strip()
    
    if mode_input == '2':
        duration = None
        print("✅ Selected: CONTINUOUS mode (Ctrl+C to stop)")
    else:
        duration_input = input("Duration (minutes, default=5): ").strip()
        try:
            duration = int(duration_input) if duration_input else 5
            duration = max(1, min(duration, 60))
        except:
            duration = 5
        print(f"✅ Selected: Timed mode ({duration} minutes)")
    
    fps_input = input("Snapshot rate (fps, default=1.0, recommend 0.5-2.0): ").strip()
    
    try:
        fps = float(fps_input) if fps_input else 1.0
        fps = max(0.1, min(fps, 5.0))
    except:
        fps = 1.0
    
    if duration is None:
        print(f"\n✅ Will collect snapshots CONTINUOUSLY at {fps} fps")
        print("   Press Ctrl+C anytime to stop")
    else:
        print(f"\n✅ Will collect snapshots for {duration} minute(s) at {fps} fps")
    
    # Start streaming
    await asyncio.sleep(2)
    success = await streamer.stream_snapshots(duration_minutes=duration, target_fps=fps)
    
    # Summary
    print("\n" + "="*60)
    if success:
        print("✅ STREAMING COMPLETED")
        print("\n📊 View results:")
        print("   Dashboard: http://localhost:8000/dashboard")
        print("   Metrics: http://localhost:8000/api/metrics/summary")
    else:
        print("❌ STREAMING FAILED")
    print("="*60)
    
    return 0 if success else 1


if __name__ == "__main__":
    import sys
    
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)