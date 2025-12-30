"""
Enhanced Privacy Manager - Phase 4
Multiple anonymization methods with configurable privacy zones
"""
import cv2
import numpy as np
import logging
from typing import List, Tuple, Dict, Any

logger = logging.getLogger("privacy_manager")


class EnhancedPrivacyManager:
    """
    Phase 4: Advanced privacy features
    - Multiple anonymization methods (blur, pixelate, black_box)
    - Configurable privacy zones
    - PII detection (faces, license plates)
    - GDPR-compliant logging
    """
    
    def __init__(self):
        # Load cascades
        self.face_cascade = None
        self.plate_cascade = None
        
        try:
            # Try to load face detector
            self.face_cascade = cv2.CascadeClassifier(
                cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
            )
            logger.info("✅ Face detector loaded")
        except Exception as e:
            logger.warning(f"⚠️  Face detector not available: {e}")
        
        try:
            # Try to load plate detector (if available)
            self.plate_cascade = cv2.CascadeClassifier(
                cv2.data.haarcascades + 'haarcascade_russian_plate_number.xml'
            )
            logger.info("✅ License plate detector loaded")
        except Exception as e:
            logger.warning(f"⚠️  Plate detector not available: {e}")
        
        # Privacy zones: list of (x, y, width, height) tuples
        # These are fixed regions to always anonymize (e.g., building entrances)
        self.privacy_zones: List[Tuple[int, int, int, int]] = []
        
        # Available anonymization methods
        self.methods = {
            'blur': self._apply_blur,
            'pixelate': self._apply_pixelate,
            'black_box': self._apply_black_box,
            'white_box': self._apply_white_box
        }
        
        # Statistics for GDPR compliance logging
        self.anonymization_log = []
    
    def anonymize_frame(
        self,
        frame: np.ndarray,
        method: str = 'blur',
        detect_faces: bool = True,
        detect_plates: bool = True,
        apply_zones: bool = True
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """
        Apply anonymization to a frame
        
        Args:
            frame: Input frame (BGR)
            method: Anonymization method ('blur', 'pixelate', 'black_box', 'white_box')
            detect_faces: Enable face detection
            detect_plates: Enable license plate detection
            apply_zones: Apply fixed privacy zones
        
        Returns:
            (anonymized_frame, statistics)
        """
        if frame is None:
            return frame, {}
        
        anonymized = frame.copy()
        regions = []
        
        # Convert to grayscale for detection
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # Detect faces
        if detect_faces and self.face_cascade is not None:
            try:
                faces = self.face_cascade.detectMultiScale(
                    gray,
                    scaleFactor=1.1,
                    minNeighbors=5,
                    minSize=(30, 30)
                )
                
                for (x, y, w, h) in faces:
                    regions.append((x, y, w, h, 'face'))
                    
            except Exception as e:
                logger.error(f"Face detection failed: {e}")
        
        # Detect license plates
        if detect_plates and self.plate_cascade is not None:
            try:
                plates = self.plate_cascade.detectMultiScale(
                    gray,
                    scaleFactor=1.05,
                    minNeighbors=3,
                    minSize=(20, 20)
                )
                
                for (x, y, w, h) in plates:
                    regions.append((x, y, w, h, 'plate'))
                    
            except Exception as e:
                logger.error(f"Plate detection failed: {e}")
        
        # Add privacy zones
        if apply_zones:
            for (x, y, w, h) in self.privacy_zones:
                regions.append((x, y, w, h, 'zone'))
        
        # Apply anonymization to detected regions
        anonymization_func = self.methods.get(method, self._apply_blur)
        
        for (x, y, w, h, region_type) in regions:
            # Ensure coordinates are within frame bounds
            x = max(0, x)
            y = max(0, y)
            x2 = min(frame.shape[1], x + w)
            y2 = min(frame.shape[0], y + h)
            
            if x2 > x and y2 > y:
                roi = anonymized[y:y2, x:x2]
                if roi.size > 0:
                    anonymized[y:y2, x:x2] = anonymization_func(roi)
        
        # Compile statistics
        stats = {
            'faces_detected': len([r for r in regions if r[4] == 'face']),
            'plates_detected': len([r for r in regions if r[4] == 'plate']),
            'zones_applied': len([r for r in regions if r[4] == 'zone']),
            'total_regions': len(regions),
            'method': method,
            'regions': regions  # For debugging/visualization
        }
        
        # Log for compliance (optional)
        self._log_anonymization(stats)
        
        return anonymized, stats
    
    def _apply_blur(self, roi: np.ndarray) -> np.ndarray:
        """Apply Gaussian blur"""
        if roi.size == 0:
            return roi
        
        # Adaptive kernel size based on ROI size
        kernel_size = max(21, min(roi.shape[0], roi.shape[1]) // 3)
        if kernel_size % 2 == 0:
            kernel_size += 1
        
        return cv2.GaussianBlur(roi, (kernel_size, kernel_size), 30)
    
    def _apply_pixelate(self, roi: np.ndarray) -> np.ndarray:
        """Apply pixelation effect"""
        if roi.size == 0:
            return roi
        
        h, w = roi.shape[:2]
        
        # Determine pixelation factor
        pixel_size = max(10, min(w, h) // 20)
        
        # Downsample
        small_w = max(1, w // pixel_size)
        small_h = max(1, h // pixel_size)
        
        temp = cv2.resize(roi, (small_w, small_h), interpolation=cv2.INTER_LINEAR)
        
        # Upsample with nearest neighbor for blocky effect
        return cv2.resize(temp, (w, h), interpolation=cv2.INTER_NEAREST)
    
    def _apply_black_box(self, roi: np.ndarray) -> np.ndarray:
        """Replace with black box"""
        return np.zeros_like(roi)
    
    def _apply_white_box(self, roi: np.ndarray) -> np.ndarray:
        """Replace with white box"""
        return np.ones_like(roi) * 255
    
    def add_privacy_zone(self, x: int, y: int, width: int, height: int):
        """
        Add a fixed privacy zone
        
        Example: Building entrance at (100, 200) with size 300x400
        """
        self.privacy_zones.append((x, y, width, height))
        logger.info(f"Added privacy zone: ({x}, {y}) {width}x{height}")
    
    def remove_privacy_zone(self, index: int):
        """Remove a privacy zone by index"""
        if 0 <= index < len(self.privacy_zones):
            zone = self.privacy_zones.pop(index)
            logger.info(f"Removed privacy zone: {zone}")
    
    def clear_privacy_zones(self):
        """Clear all privacy zones"""
        self.privacy_zones.clear()
        logger.info("Cleared all privacy zones")
    
    def _log_anonymization(self, stats: Dict[str, Any]):
        """
        Log anonymization for GDPR compliance
        Keep last 1000 entries
        """
        from datetime import datetime
        
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'faces': stats['faces_detected'],
            'plates': stats['plates_detected'],
            'zones': stats['zones_applied'],
            'method': stats['method']
        }
        
        self.anonymization_log.append(log_entry)
        
        # Keep only last 1000 entries
        if len(self.anonymization_log) > 1000:
            self.anonymization_log.pop(0)
    
    def get_anonymization_stats(self) -> Dict[str, Any]:
        """Get aggregated anonymization statistics"""
        if not self.anonymization_log:
            return {
                'total_operations': 0,
                'total_faces': 0,
                'total_plates': 0,
                'total_zones': 0
            }
        
        return {
            'total_operations': len(self.anonymization_log),
            'total_faces': sum(entry['faces'] for entry in self.anonymization_log),
            'total_plates': sum(entry['plates'] for entry in self.anonymization_log),
            'total_zones': sum(entry['zones'] for entry in self.anonymization_log),
            'methods_used': list(set(entry['method'] for entry in self.anonymization_log))
        }
    
    def visualize_detections(
        self, 
        frame: np.ndarray, 
        stats: Dict[str, Any]
    ) -> np.ndarray:
        """
        Draw bounding boxes around detected regions (for debugging)
        Returns frame with visualizations
        """
        vis = frame.copy()
        
        if 'regions' not in stats:
            return vis
        
        colors = {
            'face': (0, 255, 0),      # Green
            'plate': (0, 0, 255),     # Red
            'zone': (255, 0, 255)     # Magenta
        }
        
        for (x, y, w, h, region_type) in stats['regions']:
            color = colors.get(region_type, (255, 255, 255))
            cv2.rectangle(vis, (x, y), (x+w, y+h), color, 2)
            cv2.putText(
                vis, 
                region_type, 
                (x, y-5), 
                cv2.FONT_HERSHEY_SIMPLEX, 
                0.5, 
                color, 
                2
            )
        
        return vis


# Example usage and testing
if __name__ == "__main__":
    import sys
    
    # Test with sample image
    privacy_mgr = EnhancedPrivacyManager()
    
    # Add a sample privacy zone
    privacy_mgr.add_privacy_zone(100, 100, 200, 200)
    
    # Test with a sample frame
    test_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
    
    # Test all methods
    for method in ['blur', 'pixelate', 'black_box', 'white_box']:
        print(f"\nTesting {method}...")
        anonymized, stats = privacy_mgr.anonymize_frame(
            test_frame,
            method=method,
            detect_faces=True,
            detect_plates=False
        )
        
        print(f"Stats: {stats}")
    
    # Print overall statistics
    print(f"\nOverall statistics:")
    print(privacy_mgr.get_anonymization_stats())