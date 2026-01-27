# BioTrack AI
## Near Real-Time Biofuel Feedstock Rail Monitoring System

---

## Overview

BioTrack AI estimates volumes of biofuel feedstocks (UCO, animal fats, vegetable oils) moving through rail networks by monitoring public rail camera feeds and using computer vision to detect and classify tank cars.

### The Problem

The biofuel industry ($140B+ globally) suffers from supply chain opacity:
- Traditional data (USDA reports) lags by months
- Pricing volatility from information asymmetry
- Traders, refiners, and investors lack real-time visibility

### The Solution

Monitor rail freight—the primary transport mode for feedstocks:
1. **Capture** frames from public rail cameras at strategic locations
2. **Detect** rail cars using YOLOv8 computer vision
3. **Classify** car types (tank cars = liquid commodities)
4. **Estimate** volumes (car count × average capacity)
5. **Analyze** flows for predictive insights

---

## Quick Start

### 1. Install Dependencies

```bash
cd biotrack
pip install -r requirements.txt
```

Required packages:
```
ultralytics>=8.0.0    # YOLOv8
opencv-python>=4.8.0
pytesseract>=0.3.10   # OCR for car markings
pandas>=2.0.0
streamlit>=1.28.0     # Dashboard
geopandas>=0.14.0     # Mapping
yt-dlp>=2023.11.0     # YouTube stream capture
Pillow>=10.0.0
```

### 2. Initialize the System

```bash
python biotrack/biotrack_ai.py --init
```

This creates:
- Database tables
- Sample camera configurations
- Required directories

### 3. View System Status

```bash
python biotrack/biotrack_ai.py --summary
```

### 4. Run Analysis (Simulation Mode)

```bash
# Scrape frames (placeholder in simulation)
python biotrack/biotrack_ai.py --scrape

# Analyze with simulated detections
python biotrack/biotrack_ai.py --analyze

# View estimates
python biotrack/biotrack_ai.py --estimate
```

### 5. Launch Dashboard

```bash
python biotrack/biotrack_ai.py --dashboard
streamlit run biotrack/dashboard.py
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA COLLECTION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│   │ RailStream   │    │Virtual       │    │ Private      │      │
│   │ (48 cams)    │    │Railfan (100+)│    │ Cameras      │      │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│          │                   │                    │              │
│          └───────────────────┼────────────────────┘              │
│                              │                                   │
│                              ▼                                   │
│                    ┌──────────────────┐                          │
│                    │  Frame Scraper   │                          │
│                    │  (yt-dlp/ffmpeg) │                          │
│                    └────────┬─────────┘                          │
│                             │                                    │
└─────────────────────────────┼────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        AI ANALYSIS                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────────────────────────────────────────────┐      │
│   │                 YOLOv8 Detection                      │      │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │      │
│   │  │Tank Cars │  │ Hoppers  │  │ Boxcars  │ ...        │      │
│   │  └──────────┘  └──────────┘  └──────────┘            │      │
│   └──────────────────────────────────────────────────────┘      │
│                              │                                   │
│                              ▼                                   │
│   ┌──────────────────────────────────────────────────────┐      │
│   │              Tesseract OCR (Car Marks)                │      │
│   │              e.g., "UTLX 12345"                        │      │
│   └──────────────────────────────────────────────────────┘      │
│                              │                                   │
│                              ▼                                   │
│   ┌──────────────────────────────────────────────────────┐      │
│   │           Commodity Inference Engine                  │      │
│   │  • Location-based (near UCO collector?)               │      │
│   │  • Car type (tank car → liquid commodity)             │      │
│   │  • Historical patterns                                 │      │
│   └──────────────────────────────────────────────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      VOLUME ESTIMATION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Tank Cars Detected × Avg Capacity (30,000 gal) = Volume       │
│                                                                  │
│   ┌─────────────────────────────────────────────────────┐       │
│   │  Example:                                            │       │
│   │  15 tank cars × 30,000 gal = 450,000 gallons        │       │
│   │  450,000 gal × 0.0034 = 1,530 MT                    │       │
│   └─────────────────────────────────────────────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATABASE & OUTPUT                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│   │   SQLite     │    │  Streamlit   │    │    RLC       │      │
│   │   Database   │───▶│  Dashboard   │    │  Integration │      │
│   └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Components

### 1. Camera Database

Pre-configured cameras at strategic locations:

| Location | Railroad | Nearby Facilities | Traffic |
|----------|----------|-------------------|---------|
| Rochelle, IL | BNSF/UP | ADM, Cargill | High |
| Chesterton, IN | NS | BP Whiting | 70+/day |
| Folkston, GA | CSX | Jax/Savannah Ports | High |
| LaPlace, LA | UP/BNSF | Gulf Refineries | Critical |
| Galesburg, IL | BNSF | ADM, Bunge | High |

### 2. Detection Model

Using YOLOv8 fine-tuned on railway car dataset:
- **Source**: [Roboflow Railroad Car Detection](https://universe.roboflow.com/innopolis-university-aurlu/railroad-car-detection)
- **Classes**: Tank car, hopper, boxcar, gondola, flatcar, etc.
- **Performance**: 85%+ mAP on test set

### 3. Volume Estimation

```python
# Standard tank car capacities
DOT-111: 30,000 gallons
DOT-117: 29,000 gallons
DOT-105: 33,500 gallons
Average: 30,000 gallons

# Conversion to metric tons (oils)
1 gallon ≈ 0.0034 MT (varies by density)
```

---

## Facility Database

Key biofuel value chain nodes:

### UCO Collectors/Aggregators
| Company | Location | Rail Served |
|---------|----------|-------------|
| Mahoney Environmental | Joliet, IL | Yes |
| Sanimax | DeForest, WI | Yes |
| Baker Commodities | Kerman, CA | Yes |
| DAR PRO | National | Varies |

### Biofuel Refineries
| Facility | Location | Capacity | Railroad |
|----------|----------|----------|----------|
| Diamond Green Diesel | Norco, LA | 290K bpd | UP |
| Marathon Martinez | Martinez, CA | 48K bpd | UP |
| REG Geismar | Gonzales, LA | 90M gal/yr | UP |
| Valero DGD | Port Arthur, TX | 470M gal/yr | UP/BNSF |

---

## Global Expansion

### Brazil
- **Rail Network**: Norte-Sul Railway (soy oil)
- **Key Routes**: São Paulo → Porto Alegre
- **Commodities**: Soy oil, biodiesel

### Indonesia
- **Rail Network**: Java/Sumatra lines
- **Key Routes**: Medan → Belawan Port
- **Commodities**: Palm oil

### European Union
- **Corridors**: Mediterranean, North Sea-Baltic
- **Key Points**: Rotterdam, Hamburg
- **Compliance**: EU AI Act (explainable AI)

---

## Suggested Enhancements

### Phase 2: Advanced Features
1. **Satellite Integration**: Sentinel-2 for facility inventory
2. **IoT Sensors**: Vibration/acoustic for train detection
3. **Multi-camera Fusion**: Track same train across locations
4. **Predictive Models**: Forecast volumes based on patterns

### Phase 3: Intelligence Layer
1. **Anomaly Detection**: Unusual traffic patterns
2. **Route Optimization**: Predict delivery times
3. **Price Correlation**: Link volumes to price movements
4. **Blockchain Verification**: Immutable data trail

---

## Research Sources

- [BNSF + Roboflow Case Study](https://roboflow.com/case-studies/bnsf)
- [Virtual Railfan](https://virtualrailfan.com/)
- [RailStream Live Cameras](https://railstream.net/live-cameras)
- [FRA Rail Data](https://railroads.dot.gov/)
- [Roboflow Railroad Dataset](https://universe.roboflow.com/innopolis-university-aurlu/railroad-car-detection)

---

## Compliance Notes

- **Privacy**: No personal data collected; only public camera feeds
- **EU AI Act**: Model decisions explainable via SHAP values
- **Rate Limiting**: Respectful scraping with delays
- **Local Processing**: All analysis runs on-device

---

## Commands Reference

```bash
# Initialize system
python biotrack/biotrack_ai.py --init

# Scrape frames from cameras
python biotrack/biotrack_ai.py --scrape

# Analyze captured frames
python biotrack/biotrack_ai.py --analyze

# View daily estimates
python biotrack/biotrack_ai.py --estimate

# System summary
python biotrack/biotrack_ai.py --summary

# Create dashboard
python biotrack/biotrack_ai.py --dashboard

# Continuous monitoring (every 15 min)
python biotrack/biotrack_ai.py --continuous
```

---

*BioTrack AI - Bringing transparency to biofuel supply chains*
