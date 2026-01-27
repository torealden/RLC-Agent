#!/usr/bin/env python3
"""
BioTrack AI - Biofuel Feedstock Rail Monitoring System

A local, open-source system for estimating near-real-time volumes of biofuel
feedstocks (UCO, animal fats, greases, vegetable oils) moving through rail networks.

Features:
- Public rail camera feed scraping
- YOLOv8-based railcar detection and classification
- Volume estimation based on car counts and capacities
- SQLite database for tracking
- Integration with RLC commodity models

Usage:
    python biotrack/biotrack_ai.py --init              # Initialize database
    python biotrack/biotrack_ai.py --scrape            # Scrape camera feeds
    python biotrack/biotrack_ai.py --analyze           # Analyze captured frames
    python biotrack/biotrack_ai.py --estimate          # Generate volume estimates
    python biotrack/biotrack_ai.py --dashboard         # Launch Streamlit dashboard

Author: RLC-Agent
License: MIT
"""

import argparse
import json
import sqlite3
import hashlib
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
import urllib.request
import urllib.error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BioTrack')

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
BIOTRACK_DIR = Path(__file__).parent
DATA_DIR = BIOTRACK_DIR / "data"
FRAMES_DIR = DATA_DIR / "frames"
MODELS_DIR = BIOTRACK_DIR / "models"
DB_PATH = DATA_DIR / "biotrack.db"
CONFIG_PATH = BIOTRACK_DIR / "config"


class CarType(Enum):
    """Types of rail cars we track."""
    TANK_CAR = "tank_car"           # Liquid commodities (oils, UCO)
    HOPPER = "hopper"               # Dry bulk (grains)
    COVERED_HOPPER = "covered_hopper"
    GONDOLA = "gondola"
    BOXCAR = "boxcar"
    FLATCAR = "flatcar"
    AUTORACK = "autorack"
    INTERMODAL = "intermodal"
    LOCOMOTIVE = "locomotive"
    UNKNOWN = "unknown"


class CommodityType(Enum):
    """Commodities we're tracking for biofuels."""
    UCO = "used_cooking_oil"        # Used Cooking Oil
    TALLOW = "tallow"               # Animal fats
    YELLOW_GREASE = "yellow_grease"
    SOY_OIL = "soybean_oil"
    CORN_OIL = "corn_oil"
    CANOLA_OIL = "canola_oil"
    PALM_OIL = "palm_oil"
    BIODIESEL = "biodiesel"
    ETHANOL = "ethanol"
    UNKNOWN = "unknown"


@dataclass
class RailCamera:
    """A rail camera monitoring location."""
    camera_id: str
    name: str
    location: str
    latitude: float
    longitude: float
    source_type: str        # 'railstream', 'virtualrailfan', 'youtube', 'private'
    source_url: str
    railroad: str           # 'BNSF', 'UP', 'NS', 'CSX', etc.
    nearby_facilities: List[str]
    active: bool = True
    notes: str = ""


@dataclass
class CapturedFrame:
    """A captured frame from a rail camera."""
    frame_id: str
    camera_id: str
    timestamp: str
    file_path: str
    processed: bool = False
    car_count: int = 0
    tank_car_count: int = 0


@dataclass
class Detection:
    """A detected rail car."""
    detection_id: str
    frame_id: str
    car_type: str
    confidence: float
    bbox_x: int
    bbox_y: int
    bbox_width: int
    bbox_height: int
    car_mark: str = None        # OCR-detected marking
    inferred_commodity: str = None
    direction: str = None       # 'eastbound', 'westbound', etc.


@dataclass
class VolumeEstimate:
    """An estimated volume of commodity movement."""
    estimate_id: str
    camera_id: str
    date: str
    hour: int
    commodity: str
    car_count: int
    estimated_volume_gallons: float
    estimated_volume_mt: float
    confidence: float
    direction: str = None


# Standard tank car capacities (gallons)
TANK_CAR_CAPACITIES = {
    "DOT-111": 30_000,          # Older general purpose
    "DOT-117": 29_000,          # Newer, safer design
    "DOT-105": 33_500,          # Pressure cars
    "average": 30_000,          # Default assumption
}

# Gallons to metric tons conversion (approximate for oils)
GALLONS_TO_MT = {
    CommodityType.UCO.value: 0.00340,           # ~7.5 lbs/gal
    CommodityType.SOY_OIL.value: 0.00345,
    CommodityType.TALLOW.value: 0.00320,
    CommodityType.YELLOW_GREASE.value: 0.00330,
    CommodityType.BIODIESEL.value: 0.00330,
    "default": 0.00340,
}


class BioTrackDatabase:
    """Database manager for BioTrack."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def init_database(self):
        """Initialize all database tables."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.executescript("""
            -- Rail cameras we're monitoring
            CREATE TABLE IF NOT EXISTS cameras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                camera_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                location TEXT,
                latitude REAL,
                longitude REAL,
                source_type TEXT,
                source_url TEXT,
                railroad TEXT,
                nearby_facilities TEXT,  -- JSON array
                active INTEGER DEFAULT 1,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_cameras_railroad ON cameras(railroad);
            CREATE INDEX IF NOT EXISTS idx_cameras_active ON cameras(active);

            -- Captured frames from cameras
            CREATE TABLE IF NOT EXISTS frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                frame_id TEXT UNIQUE NOT NULL,
                camera_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                file_path TEXT,
                processed INTEGER DEFAULT 0,
                car_count INTEGER DEFAULT 0,
                tank_car_count INTEGER DEFAULT 0,
                processing_time_ms INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (camera_id) REFERENCES cameras(camera_id)
            );

            CREATE INDEX IF NOT EXISTS idx_frames_camera ON frames(camera_id);
            CREATE INDEX IF NOT EXISTS idx_frames_timestamp ON frames(timestamp);
            CREATE INDEX IF NOT EXISTS idx_frames_processed ON frames(processed);

            -- Detected rail cars
            CREATE TABLE IF NOT EXISTS detections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detection_id TEXT UNIQUE NOT NULL,
                frame_id TEXT NOT NULL,
                car_type TEXT NOT NULL,
                confidence REAL,
                bbox_x INTEGER,
                bbox_y INTEGER,
                bbox_width INTEGER,
                bbox_height INTEGER,
                car_mark TEXT,
                inferred_commodity TEXT,
                direction TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (frame_id) REFERENCES frames(frame_id)
            );

            CREATE INDEX IF NOT EXISTS idx_detections_frame ON detections(frame_id);
            CREATE INDEX IF NOT EXISTS idx_detections_type ON detections(car_type);
            CREATE INDEX IF NOT EXISTS idx_detections_commodity ON detections(inferred_commodity);

            -- Aggregated volume estimates
            CREATE TABLE IF NOT EXISTS volume_estimates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                estimate_id TEXT UNIQUE NOT NULL,
                camera_id TEXT NOT NULL,
                date DATE NOT NULL,
                hour INTEGER,
                commodity TEXT,
                car_count INTEGER,
                estimated_volume_gallons REAL,
                estimated_volume_mt REAL,
                confidence REAL,
                direction TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (camera_id) REFERENCES cameras(camera_id)
            );

            CREATE INDEX IF NOT EXISTS idx_estimates_camera ON volume_estimates(camera_id);
            CREATE INDEX IF NOT EXISTS idx_estimates_date ON volume_estimates(date);
            CREATE INDEX IF NOT EXISTS idx_estimates_commodity ON volume_estimates(commodity);

            -- Facility database
            CREATE TABLE IF NOT EXISTS facilities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facility_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                company TEXT,
                facility_type TEXT,  -- 'collector', 'refinery', 'crusher', 'renderer'
                address TEXT,
                city TEXT,
                state TEXT,
                country TEXT DEFAULT 'US',
                latitude REAL,
                longitude REAL,
                commodities_handled TEXT,  -- JSON array
                rail_served INTEGER DEFAULT 0,
                railroad TEXT,
                annual_capacity_mt REAL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_facilities_type ON facilities(facility_type);
            CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities(state);
            CREATE INDEX IF NOT EXISTS idx_facilities_rail ON facilities(rail_served);

            -- Processing log
            CREATE TABLE IF NOT EXISTS processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id TEXT UNIQUE NOT NULL,
                operation TEXT NOT NULL,
                camera_id TEXT,
                frames_processed INTEGER,
                cars_detected INTEGER,
                errors INTEGER DEFAULT 0,
                duration_seconds REAL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")

    def add_camera(self, camera: RailCamera) -> str:
        """Add a camera to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO cameras
            (camera_id, name, location, latitude, longitude, source_type,
             source_url, railroad, nearby_facilities, active, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            camera.camera_id,
            camera.name,
            camera.location,
            camera.latitude,
            camera.longitude,
            camera.source_type,
            camera.source_url,
            camera.railroad,
            json.dumps(camera.nearby_facilities),
            1 if camera.active else 0,
            camera.notes
        ))

        conn.commit()
        conn.close()
        return camera.camera_id

    def get_active_cameras(self) -> List[RailCamera]:
        """Get all active cameras."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            SELECT camera_id, name, location, latitude, longitude,
                   source_type, source_url, railroad, nearby_facilities,
                   active, notes
            FROM cameras WHERE active = 1
        """)

        cameras = []
        for row in cursor.fetchall():
            cameras.append(RailCamera(
                camera_id=row[0],
                name=row[1],
                location=row[2],
                latitude=row[3],
                longitude=row[4],
                source_type=row[5],
                source_url=row[6],
                railroad=row[7],
                nearby_facilities=json.loads(row[8]) if row[8] else [],
                active=bool(row[9]),
                notes=row[10] or ""
            ))

        conn.close()
        return cameras

    def save_frame(self, frame: CapturedFrame):
        """Save a captured frame."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO frames
            (frame_id, camera_id, timestamp, file_path, processed,
             car_count, tank_car_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            frame.frame_id,
            frame.camera_id,
            frame.timestamp,
            frame.file_path,
            1 if frame.processed else 0,
            frame.car_count,
            frame.tank_car_count
        ))

        conn.commit()
        conn.close()

    def save_detection(self, detection: Detection):
        """Save a detection."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO detections
            (detection_id, frame_id, car_type, confidence,
             bbox_x, bbox_y, bbox_width, bbox_height,
             car_mark, inferred_commodity, direction)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            detection.detection_id,
            detection.frame_id,
            detection.car_type,
            detection.confidence,
            detection.bbox_x,
            detection.bbox_y,
            detection.bbox_width,
            detection.bbox_height,
            detection.car_mark,
            detection.inferred_commodity,
            detection.direction
        ))

        conn.commit()
        conn.close()

    def save_volume_estimate(self, estimate: VolumeEstimate):
        """Save a volume estimate."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO volume_estimates
            (estimate_id, camera_id, date, hour, commodity,
             car_count, estimated_volume_gallons, estimated_volume_mt,
             confidence, direction)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            estimate.estimate_id,
            estimate.camera_id,
            estimate.date,
            estimate.hour,
            estimate.commodity,
            estimate.car_count,
            estimate.estimated_volume_gallons,
            estimate.estimated_volume_mt,
            estimate.confidence,
            estimate.direction
        ))

        conn.commit()
        conn.close()

    def get_daily_summary(self, date: str = None) -> Dict:
        """Get daily volume summary."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            SELECT commodity, SUM(car_count), SUM(estimated_volume_gallons),
                   SUM(estimated_volume_mt)
            FROM volume_estimates
            WHERE date = ?
            GROUP BY commodity
        """, (date,))

        summary = {
            "date": date,
            "commodities": {}
        }

        for row in cursor.fetchall():
            summary["commodities"][row[0]] = {
                "car_count": row[1],
                "volume_gallons": row[2],
                "volume_mt": row[3]
            }

        conn.close()
        return summary


class FrameScraper:
    """Scrapes frames from rail camera feeds."""

    def __init__(self, db: BioTrackDatabase):
        self.db = db
        FRAMES_DIR.mkdir(parents=True, exist_ok=True)

    def generate_frame_id(self, camera_id: str, timestamp: str) -> str:
        """Generate unique frame ID."""
        hash_input = f"{camera_id}_{timestamp}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]

    def scrape_railstream(self, camera: RailCamera) -> Optional[CapturedFrame]:
        """
        Scrape a frame from RailStream.

        Note: This is a placeholder. In production, you would:
        1. Use yt-dlp for YouTube streams
        2. Use requests for static image URLs
        3. Implement proper rate limiting
        """
        logger.info(f"Scraping RailStream camera: {camera.name}")

        # Placeholder - in production, implement actual scraping
        # Example for YouTube streams:
        # yt-dlp --get-url "youtube_url" -> get stream URL
        # ffmpeg -i stream_url -frames:v 1 output.jpg

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        frame_id = self.generate_frame_id(camera.camera_id, timestamp)

        # Create placeholder frame record
        frame = CapturedFrame(
            frame_id=frame_id,
            camera_id=camera.camera_id,
            timestamp=timestamp,
            file_path=str(FRAMES_DIR / f"{frame_id}.jpg"),
            processed=False
        )

        return frame

    def scrape_all_cameras(self) -> List[CapturedFrame]:
        """Scrape frames from all active cameras."""
        cameras = self.db.get_active_cameras()
        frames = []

        for camera in cameras:
            try:
                if camera.source_type == 'railstream':
                    frame = self.scrape_railstream(camera)
                elif camera.source_type == 'virtualrailfan':
                    frame = self.scrape_railstream(camera)  # Similar process
                elif camera.source_type == 'youtube':
                    frame = self.scrape_railstream(camera)  # Use yt-dlp
                else:
                    logger.warning(f"Unknown source type: {camera.source_type}")
                    continue

                if frame:
                    self.db.save_frame(frame)
                    frames.append(frame)

            except Exception as e:
                logger.error(f"Error scraping {camera.name}: {e}")

        return frames


class RailCarDetector:
    """
    Detects and classifies rail cars using computer vision.

    Uses YOLOv8 for object detection. In production:
    1. Fine-tune on Roboflow railroad car dataset
    2. Add OCR for car markings
    3. Implement direction detection via optical flow
    """

    def __init__(self):
        self.model = None
        self.model_loaded = False

    def load_model(self, model_path: str = None):
        """
        Load the YOLOv8 model.

        In production:
            from ultralytics import YOLO
            self.model = YOLO(model_path or 'yolov8n.pt')
        """
        logger.info("Loading detection model...")

        try:
            # Attempt to load ultralytics if installed
            from ultralytics import YOLO

            if model_path and Path(model_path).exists():
                self.model = YOLO(model_path)
            else:
                # Use pretrained model
                self.model = YOLO('yolov8n.pt')

            self.model_loaded = True
            logger.info("Model loaded successfully")

        except ImportError:
            logger.warning("Ultralytics not installed. Running in simulation mode.")
            logger.warning("Install with: pip install ultralytics")
            self.model_loaded = False

    def detect_cars(self, frame: CapturedFrame) -> List[Detection]:
        """
        Detect rail cars in a frame.

        Returns list of Detection objects.
        """
        detections = []

        if not self.model_loaded:
            # Simulation mode - return mock detections
            logger.info(f"Simulating detection for frame {frame.frame_id}")
            return self._simulate_detections(frame)

        try:
            # Run inference
            results = self.model(frame.file_path)

            for result in results:
                for box in result.boxes:
                    detection = Detection(
                        detection_id=f"{frame.frame_id}_{len(detections)}",
                        frame_id=frame.frame_id,
                        car_type=self._map_class_to_car_type(int(box.cls)),
                        confidence=float(box.conf),
                        bbox_x=int(box.xyxy[0][0]),
                        bbox_y=int(box.xyxy[0][1]),
                        bbox_width=int(box.xyxy[0][2] - box.xyxy[0][0]),
                        bbox_height=int(box.xyxy[0][3] - box.xyxy[0][1])
                    )
                    detections.append(detection)

        except Exception as e:
            logger.error(f"Detection error: {e}")

        return detections

    def _map_class_to_car_type(self, class_id: int) -> str:
        """Map YOLO class ID to CarType."""
        # This mapping depends on your trained model
        class_mapping = {
            0: CarType.TANK_CAR.value,
            1: CarType.HOPPER.value,
            2: CarType.COVERED_HOPPER.value,
            3: CarType.GONDOLA.value,
            4: CarType.BOXCAR.value,
            5: CarType.FLATCAR.value,
            6: CarType.AUTORACK.value,
            7: CarType.INTERMODAL.value,
            8: CarType.LOCOMOTIVE.value,
        }
        return class_mapping.get(class_id, CarType.UNKNOWN.value)

    def _simulate_detections(self, frame: CapturedFrame) -> List[Detection]:
        """Generate simulated detections for testing."""
        import random

        num_cars = random.randint(0, 15)
        detections = []

        for i in range(num_cars):
            car_type = random.choice([
                CarType.TANK_CAR.value,
                CarType.HOPPER.value,
                CarType.COVERED_HOPPER.value,
                CarType.BOXCAR.value,
            ])

            detection = Detection(
                detection_id=f"{frame.frame_id}_{i}",
                frame_id=frame.frame_id,
                car_type=car_type,
                confidence=random.uniform(0.7, 0.99),
                bbox_x=random.randint(0, 1000),
                bbox_y=random.randint(0, 500),
                bbox_width=random.randint(100, 300),
                bbox_height=random.randint(50, 150),
                direction=random.choice(['eastbound', 'westbound'])
            )
            detections.append(detection)

        return detections


class VolumeEstimator:
    """Estimates commodity volumes from car detections."""

    def __init__(self, db: BioTrackDatabase):
        self.db = db

    def estimate_from_detections(
        self,
        detections: List[Detection],
        camera_id: str,
        infer_commodity: bool = True
    ) -> VolumeEstimate:
        """
        Estimate volume from detected cars.

        Args:
            detections: List of car detections
            camera_id: Camera that captured the frame
            infer_commodity: Whether to infer commodity from location
        """
        # Count tank cars (our primary interest for biofuels)
        tank_cars = [d for d in detections if d.car_type == CarType.TANK_CAR.value]
        tank_count = len(tank_cars)

        if tank_count == 0:
            return None

        # Calculate volume
        avg_capacity = TANK_CAR_CAPACITIES["average"]
        total_gallons = tank_count * avg_capacity

        # Convert to metric tons (assume UCO/oils by default)
        conversion = GALLONS_TO_MT.get("default")
        total_mt = total_gallons * conversion

        # Infer commodity based on camera location/nearby facilities
        commodity = CommodityType.UNKNOWN.value
        if infer_commodity:
            commodity = self._infer_commodity(camera_id)

        # Average confidence
        avg_confidence = sum(d.confidence for d in tank_cars) / len(tank_cars)

        # Determine direction
        directions = [d.direction for d in tank_cars if d.direction]
        direction = max(set(directions), key=directions.count) if directions else None

        now = datetime.now()
        estimate = VolumeEstimate(
            estimate_id=f"est_{camera_id}_{now.strftime('%Y%m%d_%H%M%S')}",
            camera_id=camera_id,
            date=now.strftime("%Y-%m-%d"),
            hour=now.hour,
            commodity=commodity,
            car_count=tank_count,
            estimated_volume_gallons=total_gallons,
            estimated_volume_mt=total_mt,
            confidence=avg_confidence,
            direction=direction
        )

        return estimate

    def _infer_commodity(self, camera_id: str) -> str:
        """
        Infer commodity type based on camera location and nearby facilities.

        In production, this would:
        1. Look up nearby facilities
        2. Check if near a UCO collector, renderer, or refinery
        3. Use historical patterns for that location
        """
        # Placeholder - return UCO as most common biofuel feedstock
        return CommodityType.UCO.value


class BioTrackSystem:
    """Main system orchestrator."""

    def __init__(self):
        self.db = BioTrackDatabase()
        self.scraper = FrameScraper(self.db)
        self.detector = RailCarDetector()
        self.estimator = VolumeEstimator(self.db)

    def initialize(self):
        """Initialize the system."""
        logger.info("Initializing BioTrack AI...")

        # Create directories
        for dir_path in [DATA_DIR, FRAMES_DIR, MODELS_DIR, CONFIG_PATH]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Initialize database
        self.db.init_database()

        # Load sample cameras
        self._load_sample_cameras()

        logger.info("Initialization complete!")

    def _load_sample_cameras(self):
        """Load sample camera configurations."""
        sample_cameras = [
            RailCamera(
                camera_id="rochelle_il",
                name="Rochelle Railroad Park",
                location="Rochelle, IL",
                latitude=41.9239,
                longitude=-89.0687,
                source_type="virtualrailfan",
                source_url="https://virtualrailfan.com/rochelle-il/",
                railroad="BNSF/UP",
                nearby_facilities=["ADM Decatur", "Cargill Blair"],
                notes="High traffic crossing - BNSF and UP mainlines cross"
            ),
            RailCamera(
                camera_id="chesterton_in",
                name="Chesterton - Riley's Railhouse",
                location="Chesterton, IN",
                latitude=41.6103,
                longitude=-87.0642,
                source_type="railstream",
                source_url="https://railstream.net/live-cameras/item/chesterton-in",
                railroad="NS",
                nearby_facilities=["BP Whiting Refinery", "Calumet Specialties"],
                notes="70+ trains daily, near Chicago refineries"
            ),
            RailCamera(
                camera_id="folkston_ga",
                name="Folkston Funnel",
                location="Folkston, GA",
                latitude=30.8318,
                longitude=-82.0095,
                source_type="virtualrailfan",
                source_url="https://virtualrailfan.com/folkston-ga/",
                railroad="CSX",
                nearby_facilities=["Jacksonville Port", "Savannah Port"],
                notes="Major north-south corridor, export traffic"
            ),
            RailCamera(
                camera_id="laplace_la",
                name="LaPlace LA",
                location="LaPlace, LA",
                latitude=30.0669,
                longitude=-90.4847,
                source_type="virtualrailfan",
                source_url="https://virtualrailfan.com/laplace-la/",
                railroad="UP/BNSF",
                nearby_facilities=[
                    "Diamond Green Diesel Norco",
                    "Marathon Garyville",
                    "Valero St. Charles"
                ],
                notes="Key corridor for Gulf refineries"
            ),
            RailCamera(
                camera_id="galesburg_il",
                name="Galesburg IL",
                location="Galesburg, IL",
                latitude=40.9478,
                longitude=-90.3712,
                source_type="virtualrailfan",
                source_url="https://virtualrailfan.com/galesburg-il/",
                railroad="BNSF",
                nearby_facilities=["ADM plants", "Bunge facilities"],
                notes="BNSF hub, agricultural corridor"
            ),
        ]

        for camera in sample_cameras:
            self.db.add_camera(camera)
            logger.info(f"Added camera: {camera.name}")

    def run_scrape_cycle(self):
        """Run a single scrape cycle."""
        logger.info("Starting scrape cycle...")
        frames = self.scraper.scrape_all_cameras()
        logger.info(f"Captured {len(frames)} frames")
        return frames

    def run_analysis(self, frames: List[CapturedFrame] = None):
        """Analyze captured frames."""
        logger.info("Starting analysis...")

        # Load model
        self.detector.load_model()

        # Get unprocessed frames if none provided
        if frames is None:
            conn = sqlite3.connect(str(self.db.db_path))
            cursor = conn.cursor()
            cursor.execute("""
                SELECT frame_id, camera_id, timestamp, file_path
                FROM frames WHERE processed = 0
                ORDER BY timestamp DESC LIMIT 100
            """)

            frames = [
                CapturedFrame(
                    frame_id=row[0],
                    camera_id=row[1],
                    timestamp=row[2],
                    file_path=row[3]
                )
                for row in cursor.fetchall()
            ]
            conn.close()

        total_detections = 0
        total_tank_cars = 0

        for frame in frames:
            detections = self.detector.detect_cars(frame)

            for detection in detections:
                self.db.save_detection(detection)

            tank_count = len([d for d in detections if d.car_type == CarType.TANK_CAR.value])

            # Update frame
            frame.processed = True
            frame.car_count = len(detections)
            frame.tank_car_count = tank_count
            self.db.save_frame(frame)

            # Generate volume estimate
            if detections:
                estimate = self.estimator.estimate_from_detections(
                    detections, frame.camera_id
                )
                if estimate:
                    self.db.save_volume_estimate(estimate)

            total_detections += len(detections)
            total_tank_cars += tank_count

        logger.info(f"Analyzed {len(frames)} frames")
        logger.info(f"Total detections: {total_detections}")
        logger.info(f"Tank cars detected: {total_tank_cars}")

    def get_summary(self) -> Dict:
        """Get system summary."""
        conn = sqlite3.connect(str(self.db.db_path))
        cursor = conn.cursor()

        # Camera count
        cursor.execute("SELECT COUNT(*) FROM cameras WHERE active = 1")
        camera_count = cursor.fetchone()[0]

        # Frame count
        cursor.execute("SELECT COUNT(*) FROM frames")
        frame_count = cursor.fetchone()[0]

        # Detection count
        cursor.execute("SELECT COUNT(*) FROM detections")
        detection_count = cursor.fetchone()[0]

        # Tank car count
        cursor.execute("SELECT COUNT(*) FROM detections WHERE car_type = ?",
                      (CarType.TANK_CAR.value,))
        tank_count = cursor.fetchone()[0]

        # Today's estimates
        today = datetime.now().strftime("%Y-%m-%d")
        cursor.execute("""
            SELECT SUM(car_count), SUM(estimated_volume_gallons),
                   SUM(estimated_volume_mt)
            FROM volume_estimates WHERE date = ?
        """, (today,))

        row = cursor.fetchone()
        today_cars = row[0] or 0
        today_gallons = row[1] or 0
        today_mt = row[2] or 0

        conn.close()

        return {
            "active_cameras": camera_count,
            "total_frames": frame_count,
            "total_detections": detection_count,
            "total_tank_cars": tank_count,
            "today": {
                "date": today,
                "tank_cars": today_cars,
                "volume_gallons": today_gallons,
                "volume_mt": today_mt
            }
        }


def create_streamlit_dashboard():
    """
    Create a Streamlit dashboard for BioTrack.

    Run with: streamlit run biotrack/biotrack_ai.py dashboard
    """
    dashboard_code = '''
import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path

st.set_page_config(
    page_title="BioTrack AI",
    page_icon="🚂",
    layout="wide"
)

st.title("🚂 BioTrack AI - Biofuel Feedstock Tracker")

# Database connection
DB_PATH = Path(__file__).parent / "data" / "biotrack.db"

if not DB_PATH.exists():
    st.error("Database not found. Run: python biotrack/biotrack_ai.py --init")
    st.stop()

conn = sqlite3.connect(str(DB_PATH))

# Summary metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    cameras = pd.read_sql("SELECT COUNT(*) as n FROM cameras WHERE active=1", conn)
    st.metric("Active Cameras", cameras['n'].iloc[0])

with col2:
    frames = pd.read_sql("SELECT COUNT(*) as n FROM frames", conn)
    st.metric("Frames Captured", frames['n'].iloc[0])

with col3:
    tanks = pd.read_sql(
        "SELECT COUNT(*) as n FROM detections WHERE car_type='tank_car'", conn
    )
    st.metric("Tank Cars Detected", tanks['n'].iloc[0])

with col4:
    volume = pd.read_sql(
        "SELECT SUM(estimated_volume_mt) as v FROM volume_estimates", conn
    )
    st.metric("Est. Volume (MT)", f"{volume['v'].iloc[0] or 0:,.0f}")

st.divider()

# Daily volume chart
st.subheader("Daily Volume Estimates")
daily = pd.read_sql("""
    SELECT date, SUM(car_count) as cars,
           SUM(estimated_volume_mt) as volume_mt
    FROM volume_estimates
    GROUP BY date
    ORDER BY date DESC
    LIMIT 30
""", conn)

if not daily.empty:
    st.line_chart(daily.set_index('date')['volume_mt'])
else:
    st.info("No volume data yet. Run analysis to generate estimates.")

# Camera status
st.subheader("Camera Locations")
cameras_df = pd.read_sql("""
    SELECT name, location, railroad, source_type
    FROM cameras WHERE active = 1
""", conn)
st.dataframe(cameras_df, use_container_width=True)

# Recent detections
st.subheader("Recent Detections")
detections_df = pd.read_sql("""
    SELECT d.car_type, d.confidence, d.direction,
           f.camera_id, f.timestamp
    FROM detections d
    JOIN frames f ON d.frame_id = f.frame_id
    ORDER BY f.timestamp DESC
    LIMIT 100
""", conn)
st.dataframe(detections_df, use_container_width=True)

conn.close()
'''

    dashboard_path = BIOTRACK_DIR / "dashboard.py"
    dashboard_path.write_text(dashboard_code)
    logger.info(f"Dashboard created at: {dashboard_path}")
    logger.info("Run with: streamlit run biotrack/dashboard.py")


def main():
    parser = argparse.ArgumentParser(
        description='BioTrack AI - Biofuel Feedstock Rail Monitoring'
    )
    parser.add_argument('--init', action='store_true',
                       help='Initialize database and load sample cameras')
    parser.add_argument('--scrape', action='store_true',
                       help='Scrape frames from all active cameras')
    parser.add_argument('--analyze', action='store_true',
                       help='Analyze captured frames')
    parser.add_argument('--estimate', action='store_true',
                       help='Generate volume estimates')
    parser.add_argument('--summary', action='store_true',
                       help='Show system summary')
    parser.add_argument('--dashboard', action='store_true',
                       help='Create Streamlit dashboard')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuous monitoring (scrape every 15 min)')

    args = parser.parse_args()

    system = BioTrackSystem()

    if args.init:
        system.initialize()
        return

    if args.scrape:
        frames = system.run_scrape_cycle()
        print(f"Captured {len(frames)} frames")
        return

    if args.analyze:
        system.run_analysis()
        return

    if args.estimate:
        summary = system.db.get_daily_summary()
        print(json.dumps(summary, indent=2))
        return

    if args.summary:
        summary = system.get_summary()
        print("\n" + "="*50)
        print("  BIOTRACK AI SYSTEM SUMMARY")
        print("="*50)
        print(f"  Active Cameras:    {summary['active_cameras']}")
        print(f"  Total Frames:      {summary['total_frames']}")
        print(f"  Total Detections:  {summary['total_detections']}")
        print(f"  Tank Cars Found:   {summary['total_tank_cars']}")
        print("-"*50)
        print(f"  TODAY ({summary['today']['date']}):")
        print(f"    Tank Cars:       {summary['today']['tank_cars']}")
        print(f"    Volume (gal):    {summary['today']['volume_gallons']:,.0f}")
        print(f"    Volume (MT):     {summary['today']['volume_mt']:,.1f}")
        print("="*50 + "\n")
        return

    if args.dashboard:
        create_streamlit_dashboard()
        return

    if args.continuous:
        print("Starting continuous monitoring (Ctrl+C to stop)...")
        while True:
            try:
                frames = system.run_scrape_cycle()
                system.run_analysis(frames)
                summary = system.get_summary()
                print(f"[{datetime.now()}] Processed {len(frames)} frames, "
                      f"Today: {summary['today']['tank_cars']} tank cars")
                time.sleep(900)  # 15 minutes
            except KeyboardInterrupt:
                print("\nStopping...")
                break
        return

    # Default: show help
    parser.print_help()


if __name__ == '__main__':
    main()
