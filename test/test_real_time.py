"""
YouTube Live Stream Test with Real-time Dashboard
Tạo biểu đồ Queue Max Length và bảng số liệu thực nghiệm
"""
import asyncio
import httpx
import time
from datetime import datetime, timedelta
from pathlib import Path
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from collections import deque
import pandas as pd


class YouTubeLiveStreamTester:
    """
    Test với YouTube live stream và tạo dashboard real-time
    """
    
    def __init__(self):
        self.base_urls = {
            'orchestrator': 'http://localhost:8000',
            'ingestion': 'http://localhost:8001',
            'processing': 'http://localhost:8002',
            'storage': 'http://localhost:8003'
        }
        
        # YouTube live stream URL (ví dụ)
        # Thay thế bằng URL live stream thực tế
        self.youtube_url = "https://www.youtube.com/watch?v=YOUR_LIVE_STREAM_ID"
        
        # Hoặc dùng yt-dlp để lấy direct URL
        self.stream_url = None
        
        # Data storage cho biểu đồ
        self.time_data = deque(maxlen=1000)  # Unlimited for chart
        self.queue_data_60fps = deque(maxlen=1000)
        self.queue_data_120fps = deque(maxlen=1000)
        self.queue_data_240fps = deque(maxlen=1000)
        
        # Data cho bảng (60 phút)
        self.table_data = {
            'duration': [],  # minutes
            'fps': [],
            'frames_total': [],
            'time_taken': []  # minutes
        }
        
        # Start time
        self.start_time = None
        
        # Figure for plotting
        self.fig = None
        self.ax1 = None  # Queue chart
        self.ax2 = None  # Table
    
    def extract_stream_url(self, youtube_url: str) -> bool:
        """
        Extract direct stream URL from YouTube using yt-dlp
        """
        print(f"🔍 Extracting stream URL from: {youtube_url}")
        
        try:
            import yt_dlp
            
            ydl_opts = {
                'format': 'best[ext=mp4]',
                'quiet': True
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                self.stream_url = info['url']
                
                print(f"✅ Stream URL extracted")
                print(f"   Title: {info.get('title', 'N/A')}")
                print(f"   Duration: {info.get('duration', 'Live')}")
                
                return True
                
        except ImportError:
            print("❌ yt-dlp not installed")
            print("   Install: pip install yt-dlp")
            return False
        except Exception as e:
            print(f"❌ Failed to extract URL: {e}")
            return False
    
    async def check_services(self) -> bool:
        """Check if all services are running"""
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
    
    async def run_experiment(self, duration_minutes: int, fps: int):
        """
        Run one experiment with specified duration and FPS
        Uses direct frame capture instead of ingestion agent
        """
        print(f"\n{'='*60}")
        print(f"🧪 EXPERIMENT: {duration_minutes}min @ {fps}fps")
        print(f"{'='*60}")
        
        import cv2
        import base64
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                # Open video stream
                print(f"📹 Opening stream...")
                cap = cv2.VideoCapture(self.stream_url)
                
                if not cap.isOpened():
                    print(f"❌ Cannot open stream")
                    return None
                
                print(f"✅ Stream opened successfully")
                
                start_time = time.time()
                frames_captured = 0
                frames_sent = 0
                max_queue = 0
                batch = []
                batch_size = 10
                
                # Calculate frame interval
                target_interval = 1.0 / fps
                
                # Monitor for specified duration
                duration_seconds = duration_minutes * 60
                last_frame_time = time.time()
                
                print(f"📊 Capturing at {fps} fps for {duration_minutes} minutes...")
                
                while time.time() - start_time < duration_seconds:
                    current_time = time.time()
                    
                    # Check if it's time to capture next frame
                    if current_time - last_frame_time >= target_interval:
                        ret, frame = cap.read()
                        
                        if not ret:
                            print("⚠️ Stream ended or error reading frame")
                            break
                        
                        frames_captured += 1
                        last_frame_time = current_time
                        
                        # Encode frame
                        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                        frame_b64 = base64.b64encode(buffer).decode('utf-8')
                        
                        # Add to batch
                        batch.append({
                            "frame_id": f"youtube_{duration_minutes}min_{fps}fps_{frames_captured}",
                            "sequence_number": frames_captured,
                            "timestamp": datetime.utcnow().isoformat(),
                            "data": frame_b64,
                            "metadata": {
                                "width": frame.shape[1],
                                "height": frame.shape[0],
                                "format": "jpeg",
                                "size_bytes": len(buffer)
                            }
                        })
                        
                        # Send batch when full
                        if len(batch) >= batch_size:
                            try:
                                batch_data = {
                                    "batch_id": f"youtube_batch_{frames_sent}",
                                    "source_id": f"youtube_{duration_minutes}min_{fps}fps",
                                    "frames": batch
                                }
                                
                                await client.post(
                                    f"{self.base_urls['processing']}/process/batch",
                                    json=batch_data,
                                    timeout=30.0
                                )
                                
                                frames_sent += len(batch)
                                batch = []
                                
                            except Exception as e:
                                print(f"⚠️ Failed to send batch: {e}")
                    
                    # Get processing status
                    try:
                        proc_resp = await client.get(
                            f"{self.base_urls['processing']}/process/status",
                            timeout=5.0
                        )
                        
                        if proc_resp.status_code == 200:
                            proc_data = proc_resp.json()
                            queue_len = proc_data['queue_length']
                            max_queue = max(max_queue, queue_len)
                    except:
                        pass
                    
                    # Update real-time data every 2 seconds
                    if int(current_time - start_time) % 2 == 0:
                        elapsed = (current_time - start_time) / 60  # minutes
                        self.time_data.append(elapsed)
                        
                        if fps == 60:
                            self.queue_data_60fps.append(queue_len)
                        elif fps == 120:
                            self.queue_data_120fps.append(queue_len)
                        elif fps == 240:
                            self.queue_data_240fps.append(queue_len)
                    
                    # Print progress every 30 seconds
                    if int(current_time - start_time) % 30 == 0:
                        elapsed_min = (current_time - start_time) / 60
                        print(f"   [{elapsed_min:.1f}min] Captured: {frames_captured}, "
                              f"Sent: {frames_sent}, Queue: {queue_len}, Max: {max_queue}")
                    
                    # Small sleep to prevent CPU overload
                    await asyncio.sleep(0.001)
                
                # Send remaining frames
                if len(batch) > 0:
                    try:
                        batch_data = {
                            "batch_id": f"youtube_batch_{frames_sent}",
                            "source_id": f"youtube_{duration_minutes}min_{fps}fps",
                            "frames": batch
                        }
                        
                        await client.post(
                            f"{self.base_urls['processing']}/process/batch",
                            json=batch_data,
                            timeout=30.0
                        )
                        
                        frames_sent += len(batch)
                    except Exception as e:
                        print(f"⚠️ Failed to send final batch: {e}")
                
                cap.release()
                
                # Calculate time taken
                time_taken = (time.time() - start_time) / 60
                
                # Store results
                result = {
                    'duration': duration_minutes,
                    'fps': fps,
                    'frames_total': frames_sent,
                    'time_taken': time_taken,
                    'max_queue': max_queue
                }
                
                print(f"\n✅ Experiment completed:")
                print(f"   Duration: {duration_minutes}min")
                print(f"   Target FPS: {fps}")
                print(f"   Frames captured: {frames_captured}")
                print(f"   Frames sent: {frames_sent}")
                print(f"   Time: {time_taken:.2f}min")
                print(f"   Max Queue: {max_queue}")
                
                return result
                
        except Exception as e:
            print(f"❌ Experiment failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def run_table_experiments(self):
        """
        Run experiments for Table 2 (1, 10, 30, 60 minutes @ 60, 120, 240 fps)
        """
        print(f"\n{'='*60}")
        print("📊 RUNNING TABLE EXPERIMENTS")
        print(f"{'='*60}")
        
        durations = [1, 10, 30, 60]  # minutes
        fps_values = [60, 120, 240]
        
        for duration in durations:
            for fps in fps_values:
                result = await self.run_experiment(duration, fps)
                
                if result:
                    self.table_data['duration'].append(result['duration'])
                    self.table_data['fps'].append(result['fps'])
                    self.table_data['frames_total'].append(result['frames_total'])
                    self.table_data['time_taken'].append(result['time_taken'])
                
                # Short break between experiments
                await asyncio.sleep(10)
        
        # Save results
        self.save_table_results()
    
    def save_table_results(self):
        """
        Save table results to CSV and display
        """
        df = pd.DataFrame(self.table_data)
        
        # Sort by duration and fps
        df = df.sort_values(['duration', 'fps'])
        
        # Save to CSV
        output_file = 'experiment_results_table2.csv'
        df.to_csv(output_file, index=False)
        
        print(f"\n{'='*60}")
        print("📋 TABLE 2: EXPERIMENT RESULTS")
        print(f"{'='*60}")
        print(df.to_string(index=False))
        print(f"\n✅ Saved to: {output_file}")
    
    def setup_dashboard(self):
        """
        Setup matplotlib dashboard for real-time plotting
        """
        plt.ion()  # Interactive mode
        
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Chart 1: Queue Length
        self.ax1.set_title('Queue Max Length vs Duration', fontsize=14, fontweight='bold')
        self.ax1.set_xlabel('Duration (minutes)')
        self.ax1.set_ylabel('Queue Max Length')
        self.ax1.grid(True, alpha=0.3)
        self.ax1.legend(['60 fps', '120 fps', '240 fps'])
        
        # Chart 2: Table placeholder
        self.ax2.axis('off')
        
        plt.tight_layout()
    
    def update_dashboard(self):
        """
        Update dashboard with latest data
        """
        if not self.fig:
            return
        
        # Clear and redraw
        self.ax1.clear()
        
        # Plot queue data
        if len(self.time_data) > 0:
            time_array = np.array(self.time_data)
            
            if len(self.queue_data_60fps) > 0:
                self.ax1.plot(time_array[:len(self.queue_data_60fps)], 
                             self.queue_data_60fps, 
                             'b-', label='60 fps', linewidth=2)
            
            if len(self.queue_data_120fps) > 0:
                self.ax1.plot(time_array[:len(self.queue_data_120fps)], 
                             self.queue_data_120fps, 
                             'g-', label='120 fps', linewidth=2)
            
            if len(self.queue_data_240fps) > 0:
                self.ax1.plot(time_array[:len(self.queue_data_240fps)], 
                             self.queue_data_240fps, 
                             'r-', label='240 fps', linewidth=2)
        
        self.ax1.set_title('Queue Max Length vs Duration', fontsize=14, fontweight='bold')
        self.ax1.set_xlabel('Duration (minutes)')
        self.ax1.set_ylabel('Queue Max Length')
        self.ax1.grid(True, alpha=0.3)
        self.ax1.legend()
        
        # Update table
        self.ax2.clear()
        self.ax2.axis('off')
        
        if len(self.table_data['duration']) > 0:
            df = pd.DataFrame(self.table_data)
            
            table = self.ax2.table(
                cellText=df.values,
                colLabels=df.columns,
                cellLoc='center',
                loc='center',
                colWidths=[0.2, 0.2, 0.3, 0.3]
            )
            
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 2)
            
            # Style header
            for i in range(len(df.columns)):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')
        
        plt.draw()
        plt.pause(0.1)
    
    async def run_continuous_monitoring(self, fps: int = 60):
        """
        Run continuous monitoring for chart (unlimited duration)
        Direct frame capture instead of using ingestion agent
        """
        print(f"\n{'='*60}")
        print(f"📊 CONTINUOUS MONITORING @ {fps}fps")
        print(f"{'='*60}")
        print("Press Ctrl+C to stop")
        
        import cv2
        import base64
        
        try:
            # Open video stream
            print(f"📹 Opening stream...")
            cap = cv2.VideoCapture(self.stream_url)
            
            if not cap.isOpened():
                print(f"❌ Cannot open stream")
                return
            
            print(f"✅ Stream opened successfully")
            
            async with httpx.AsyncClient(timeout=120.0) as client:
                self.start_time = time.time()
                frames_captured = 0
                frames_sent = 0
                batch = []
                batch_size = 10
                
                # Calculate frame interval
                target_interval = 1.0 / fps
                last_frame_time = time.time()
                
                print(f"📊 Starting continuous capture at {fps} fps...")
                print(f"   Update interval: {target_interval:.3f}s per frame")
                
                # Monitor indefinitely
                while True:
                    current_time = time.time()
                    
                    # Check if it's time to capture next frame
                    if current_time - last_frame_time >= target_interval:
                        ret, frame = cap.read()
                        
                        if not ret:
                            print("⚠️ Stream ended, reconnecting...")
                            cap.release()
                            await asyncio.sleep(5)
                            cap = cv2.VideoCapture(self.stream_url)
                            continue
                        
                        frames_captured += 1
                        last_frame_time = current_time
                        
                        # Encode frame
                        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                        frame_b64 = base64.b64encode(buffer).decode('utf-8')
                        
                        # Add to batch
                        batch.append({
                            "frame_id": f"youtube_continuous_{fps}fps_{frames_captured}",
                            "sequence_number": frames_captured,
                            "timestamp": datetime.utcnow().isoformat(),
                            "data": frame_b64,
                            "metadata": {
                                "width": frame.shape[1],
                                "height": frame.shape[0],
                                "format": "jpeg",
                                "size_bytes": len(buffer)
                            }
                        })
                        
                        # Send batch when full
                        if len(batch) >= batch_size:
                            try:
                                batch_data = {
                                    "batch_id": f"youtube_continuous_{frames_sent}",
                                    "source_id": f"youtube_continuous_{fps}fps",
                                    "frames": batch
                                }
                                
                                await client.post(
                                    f"{self.base_urls['processing']}/process/batch",
                                    json=batch_data,
                                    timeout=30.0
                                )
                                
                                frames_sent += len(batch)
                                batch = []
                                
                            except Exception as e:
                                print(f"⚠️ Failed to send batch: {e}")
                    
                    # Get processing status
                    try:
                        proc_resp = await client.get(
                            f"{self.base_urls['processing']}/process/status",
                            timeout=5.0
                        )
                        
                        if proc_resp.status_code == 200:
                            proc_data = proc_resp.json()
                            queue_len = proc_data['queue_length']
                            
                            elapsed = (current_time - self.start_time) / 60
                            self.time_data.append(elapsed)
                            
                            if fps == 60:
                                self.queue_data_60fps.append(queue_len)
                            elif fps == 120:
                                self.queue_data_120fps.append(queue_len)
                            elif fps == 240:
                                self.queue_data_240fps.append(queue_len)
                            
                            # Update dashboard
                            if int(current_time - self.start_time) % 2 == 0:
                                self.update_dashboard()
                            
                            # Print status every 30 seconds
                            if int(current_time - self.start_time) % 30 == 0:
                                print(f"   [{elapsed:.1f}min] Captured: {frames_captured}, "
                                      f"Sent: {frames_sent}, Queue: {queue_len}, "
                                      f"Processed: {proc_data['processed_frames']}")
                    except:
                        pass
                    
                    # Small sleep to prevent CPU overload
                    await asyncio.sleep(0.001)
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Monitoring stopped by user")
            if cap:
                cap.release()
        except Exception as e:
            print(f"\n❌ Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
            if cap:
                cap.release()


async def main():
    """
    Main function
    """
    print("""
╔══════════════════════════════════════════════════════════╗
║     YOUTUBE LIVE STREAM TEST WITH DASHBOARD             ║
║                                                          ║
║  Creates:                                                ║
║  1. Real-time Queue Length Chart (Figure 2)              ║
║  2. Experiment Results Table (Table 2)                   ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    tester = YouTubeLiveStreamTester()
    
    # Step 1: Check dependencies
    print("\n" + "="*60)
    print("STEP 1: Check Dependencies")
    print("="*60)
    
    try:
        import yt_dlp
        print("✅ yt-dlp installed")
    except ImportError:
        print("❌ yt-dlp not installed")
        print("   Install: pip install yt-dlp")
        return 1
    
    try:
        import matplotlib
        print("✅ matplotlib installed")
    except ImportError:
        print("❌ matplotlib not installed")
        print("   Install: pip install matplotlib")
        return 1
    
    # Step 2: Get YouTube URL
    print("\n" + "="*60)
    print("STEP 2: YouTube Live Stream URL")
    print("="*60)
    
    youtube_url = input("Enter YouTube live stream URL: ").strip()
    
    if not youtube_url:
        print("❌ No URL provided")
        return 1
    
    if not tester.extract_stream_url(youtube_url):
        print("❌ Failed to extract stream URL")
        return 1
    
    # Step 3: Check services
    print("\n" + "="*60)
    print("STEP 3: Check Pipeline Services")
    print("="*60)
    
    if not await tester.check_services():
        print("❌ Services not ready")
        print("   Run: docker-compose up -d")
        return 1
    
    # Step 4: Choose mode
    print("\n" + "="*60)
    print("STEP 4: Select Test Mode")
    print("="*60)
    print("1. Run Table Experiments (1, 10, 30, 60 min @ 60, 120, 240 fps)")
    print("2. Continuous Monitoring (for real-time chart)")
    print("3. Both (Table first, then continuous)")
    
    mode = input("\nSelect mode (1-3, default=3): ").strip() or "3"
    
    # Setup dashboard
    tester.setup_dashboard()
    
    if mode in ["1", "3"]:
        # Run table experiments
        await tester.run_table_experiments()
        
        print("\n✅ Table experiments completed!")
    
    if mode in ["2", "3"]:
        # Choose FPS for continuous monitoring
        fps_input = input("\nEnter FPS for continuous monitoring (60/120/240, default=60): ").strip()
        fps = int(fps_input) if fps_input in ["60", "120", "240"] else 60
        
        # Run continuous monitoring
        await tester.run_continuous_monitoring(fps=fps)
    
    # Keep plot window open
    print("\n✅ Test completed. Close the plot window to exit.")
    plt.ioff()
    plt.show()
    
    return 0


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
        import traceback
        traceback.print_exc()
        sys.exit(1)