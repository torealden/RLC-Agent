"""
NASS Crop Progress & Condition Gridded Layers Collector

Manages the USDA NASS CPC GeoTIFF archive:
  - Extracts ZIP files into organized TIF directories
  - Parses GeoTIFF filenames for crop/product/year/week metadata
  - Reads raster metadata and computes statistics via rasterio
  - Runs QA validation (CRS, resolution, domain checks)
  - Computes national-level zonal statistics
  - Saves file manifest + region stats to PostgreSQL bronze layer

Archive structure:
  data/crop_progress_conditions/
    cpc2025/{crop}/{crop}{crop}2025.zip      (newer layout)
    cpc20XX/cpc20XX/{crop}/{crop}{crop}20XX.zip  (older layout)

GeoTIFF naming convention:
  {crop}{Product}{YY}w{WW}.tif
  e.g. cornCond24w18.tif = corn Condition, 2024, week 18

Requires: rasterio, numpy (for raster metadata + stats)
Graceful degradation: extraction + filename parsing work without rasterio.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import uuid
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

try:
    import rasterio
    RASTERIO_AVAILABLE = True
except ImportError:
    RASTERIO_AVAILABLE = False

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

ARCHIVE_ROOT = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\data\crop_progress_conditions")

# Maps ZIP/folder crop names to canonical DB crop names
CROP_MAP = {
    "corn": "corn",
    "soy": "soybeans",
    "cotton": "cotton",
    "wheat": "winter_wheat",
}

# Maps filename crop prefixes to canonical names (GeoTIFF filenames use these)
CROP_PREFIX_MAP = {
    "corn": "corn",
    "soy": "soybeans",
    "cotton": "cotton",
    "wheat": "winter_wheat",
}

PRODUCT_MAP = {
    "cond": "condition",
    "prog": "progress",
}

# Filename regex: cornCond24w18.tif, soyCond25w14.tif, etc.
NAME_RE = re.compile(
    r"^(?P<crop>corn|soy|cotton|wheat)"
    r"(?P<product>Cond|Prog)"
    r"(?P<yy>\d{2})"
    r"w(?P<ww>\d{2})"
    r"\.tif$",
    re.IGNORECASE
)

# Series ID template — uses short product names to match DB seed data
PRODUCT_SHORT = {"condition": "cond", "progress": "prog"}
SERIES_ID_FMT = "cpc_{product_short}_{crop}_9km_v1"

# Expected CRS identifier substrings (NAD83 Conus Albers)
EXPECTED_CRS_KEYWORDS = ["NAD83", "Albers", "USA_Contiguous"]

# NASS survey season: weeks typically run April (week ~14) through November (week ~48)
NASS_SEASON_WEEK_MIN = 10
NASS_SEASON_WEEK_MAX = 52

COLLECTOR_VERSION = "1.0.0"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TifMetadata:
    """Metadata for a single GeoTIFF file."""
    file_path: str
    file_name: str
    file_sha256: str = ""
    file_bytes: int = 0
    modified_utc: str = ""

    # Parsed from filename
    crop: str = ""
    product: str = ""
    year: int = 0
    nass_week: int = 0
    week_ending_date: Optional[str] = None
    series_id: str = ""

    # Raster metadata (populated if rasterio available)
    crs_wkt: Optional[str] = None
    pixel_size_m: Optional[float] = None
    dtype: Optional[str] = None
    nodata_value: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    bbox_xmin: Optional[float] = None
    bbox_ymin: Optional[float] = None
    bbox_xmax: Optional[float] = None
    bbox_ymax: Optional[float] = None

    # Statistics
    value_min: Optional[float] = None
    value_max: Optional[float] = None
    value_mean: Optional[float] = None
    pct_nodata: Optional[float] = None

    # QA
    qa_passed: bool = True
    qa_notes: str = ""


@dataclass
class RegionStat:
    """A single zonal statistic for a region/week."""
    series_id: str
    year: int
    nass_week: int
    week_ending_date: Optional[str]
    region_id: str
    region_type: str
    stat_name: str
    value: Optional[float]
    pixel_count: Optional[int] = None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def nass_week_to_date(year: int, week: int) -> Optional[date]:
    """
    Convert NASS year + week number to the week-ending Sunday date.

    NASS weeks are ISO-ish but end on Sunday. Week 1 contains the first
    Sunday of the year. We approximate by computing the Sunday of the
    given ISO week.
    """
    try:
        # ISO week: Monday is day 1, Sunday is day 7
        # NASS week ending = Sunday of that ISO week
        # date.fromisocalendar(year, week, 7) gives Sunday
        d = date.fromisocalendar(year, week, 7)
        return d
    except (ValueError, AttributeError):
        # Fallback: Jan 1 + (week - 1) * 7, find next Sunday
        try:
            jan1 = date(year, 1, 1)
            # Days to first Sunday
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            target = first_sunday + timedelta(weeks=week - 1)
            return target
        except Exception:
            return None


# =============================================================================
# ZIP EXTRACTION
# =============================================================================

def find_all_zips(archive_root: Path, year: Optional[int] = None,
                  crop: Optional[str] = None) -> List[Path]:
    """Find all CPC ZIP files in the archive."""
    zips = []
    for p in archive_root.rglob("*.zip"):
        # Skip anything in _manifests or output dirs
        if "_manifests" in str(p) or "output" in str(p):
            continue

        # Filter by year if specified
        if year is not None:
            year_str = str(year)
            if year_str not in p.stem and year_str not in str(p.parent):
                continue

        # Filter by crop if specified
        if crop is not None:
            crop_lower = crop.lower()
            if crop_lower not in p.stem.lower():
                continue

        zips.append(p)

    return sorted(zips)


def extract_zip(zip_path: Path, force: bool = False) -> Tuple[int, Path]:
    """
    Extract a single ZIP file to a 'tif' subdirectory alongside the ZIP.

    Returns (count_of_tifs_extracted, extract_dir).
    """
    extract_dir = zip_path.parent / "tif"

    # Check if already extracted
    if not force and extract_dir.exists():
        existing_tifs = list(extract_dir.glob("*.tif"))
        if existing_tifs:
            logger.debug(f"  Already extracted ({len(existing_tifs)} TIFs): {zip_path.name}")
            return 0, extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    count = 0

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.namelist():
                if member.lower().endswith('.tif') or member.lower().endswith('.tiff'):
                    # Extract to flat directory (strip any internal path)
                    basename = Path(member).name
                    target = extract_dir / basename
                    with zf.open(member) as src, open(target, 'wb') as dst:
                        dst.write(src.read())
                    count += 1

        logger.info(f"  Extracted {count} TIFs from {zip_path.name}")
    except zipfile.BadZipFile:
        logger.error(f"  Bad ZIP file: {zip_path}")
    except Exception as e:
        logger.error(f"  Error extracting {zip_path}: {e}")

    return count, extract_dir


def extract_all_zips(archive_root: Path, year: Optional[int] = None,
                     crop: Optional[str] = None, force: bool = False) -> Dict:
    """
    Extract all ZIP files in the archive.

    Returns summary dict with counts.
    """
    zips = find_all_zips(archive_root, year=year, crop=crop)
    logger.info(f"Found {len(zips)} ZIP files to process")

    total_extracted = 0
    total_skipped = 0
    total_zips = len(zips)

    for zp in zips:
        count, _ = extract_zip(zp, force=force)
        if count > 0:
            total_extracted += count
        else:
            total_skipped += 1

    summary = {
        "zips_found": total_zips,
        "tifs_extracted": total_extracted,
        "zips_skipped": total_skipped,
    }
    logger.info(f"Extraction complete: {summary}")
    return summary


# =============================================================================
# FILENAME PARSING
# =============================================================================

def parse_tif_filename(filename: str) -> Optional[Dict]:
    """
    Parse a CPC GeoTIFF filename into metadata.

    Examples:
      cornCond24w18.tif -> {crop: corn, product: condition, year: 2024, nass_week: 18}
      soyProg25w14.tif  -> {crop: soybeans, product: progress, year: 2025, nass_week: 14}
    """
    m = NAME_RE.match(filename)
    if not m:
        return None

    crop_raw = m.group("crop").lower()
    product_raw = m.group("product").lower()
    yy = int(m.group("yy"))
    ww = int(m.group("ww"))

    crop = CROP_PREFIX_MAP.get(crop_raw, crop_raw)
    product = PRODUCT_MAP.get(product_raw, product_raw)
    year = 2000 + yy if yy < 50 else 1900 + yy

    product_short = PRODUCT_SHORT.get(product, product)
    series_id = SERIES_ID_FMT.format(product_short=product_short, crop=crop)
    week_date = nass_week_to_date(year, ww)

    return {
        "crop": crop,
        "product": product,
        "year": year,
        "nass_week": ww,
        "week_ending_date": week_date.isoformat() if week_date else None,
        "series_id": series_id,
    }


# =============================================================================
# RASTER METADATA
# =============================================================================

def read_raster_metadata(tif_path: Path) -> Dict:
    """
    Read GeoTIFF metadata and compute lightweight statistics.

    Returns dict with CRS, bounds, pixel size, dtype, stats.
    Requires rasterio.
    """
    if not RASTERIO_AVAILABLE:
        return {}

    result = {}
    try:
        with rasterio.open(tif_path) as ds:
            result["crs_wkt"] = ds.crs.to_wkt() if ds.crs else None
            result["crs_str"] = str(ds.crs) if ds.crs else None
            result["dtype"] = str(ds.dtypes[0])
            result["nodata_value"] = float(ds.nodata) if ds.nodata is not None else None
            result["width"] = ds.width
            result["height"] = ds.height
            result["pixel_size_m"] = abs(ds.transform.a)
            result["bbox_xmin"] = ds.bounds.left
            result["bbox_ymin"] = ds.bounds.bottom
            result["bbox_xmax"] = ds.bounds.right
            result["bbox_ymax"] = ds.bounds.top

            # Read band 1 for stats
            band = ds.read(1)
            nodata = ds.nodata

            if nodata is not None:
                mask = band != nodata
            else:
                mask = np.ones_like(band, dtype=bool)

            total_pixels = band.size
            valid_pixels = int(mask.sum())
            masked_pixels = total_pixels - valid_pixels

            result["pct_nodata"] = float(masked_pixels / total_pixels) if total_pixels > 0 else None

            if valid_pixels > 0:
                valid_data = band[mask]
                result["value_min"] = float(np.nanmin(valid_data))
                result["value_max"] = float(np.nanmax(valid_data))
                result["value_mean"] = float(np.nanmean(valid_data))
            else:
                result["value_min"] = None
                result["value_max"] = None
                result["value_mean"] = None

    except Exception as e:
        logger.warning(f"  Error reading raster {tif_path.name}: {e}")

    return result


def compute_national_stats(tif_path: Path) -> List[Dict]:
    """
    Compute national-level zonal statistics for a GeoTIFF.

    Returns list of stat dicts (mean, median, p10, p90, pct_nodata, pixel_count).
    """
    if not RASTERIO_AVAILABLE:
        return []

    stats = []
    try:
        with rasterio.open(tif_path) as ds:
            band = ds.read(1)
            nodata = ds.nodata

            if nodata is not None:
                mask = band != nodata
            else:
                mask = np.ones_like(band, dtype=bool)

            total_pixels = band.size
            valid_pixels = int(mask.sum())

            if valid_pixels == 0:
                return []

            valid_data = band[mask].astype(np.float64)

            stat_values = {
                "mean": float(np.nanmean(valid_data)),
                "median": float(np.nanmedian(valid_data)),
                "p10": float(np.nanpercentile(valid_data, 10)),
                "p90": float(np.nanpercentile(valid_data, 90)),
                "pct_nodata": float((total_pixels - valid_pixels) / total_pixels),
                "pixel_count": float(valid_pixels),
            }

            for stat_name, value in stat_values.items():
                stats.append({
                    "region_id": "US",
                    "region_type": "national",
                    "stat_name": stat_name,
                    "value": value,
                    "pixel_count": valid_pixels,
                })

    except Exception as e:
        logger.warning(f"  Error computing stats for {tif_path.name}: {e}")

    return stats


# =============================================================================
# QA CHECKS
# =============================================================================

def run_qa_checks(meta: TifMetadata) -> Tuple[bool, str]:
    """
    Run QA validation on a TIF's metadata.

    Checks:
      1. CRS contains NAD83 + Albers keywords
      2. Pixel size ~= 9000m
      3. dtype = float32
      4. Domain: condition [1,5], progress [0,1]

    Returns (passed, notes_string).
    """
    issues = []

    # CRS check
    if meta.crs_wkt:
        crs_upper = meta.crs_wkt.upper()
        if "NAD" not in crs_upper or "ALBERS" not in crs_upper:
            issues.append(f"CRS mismatch: expected NAD83 Albers")
    else:
        issues.append("CRS not available")

    # Pixel size check
    if meta.pixel_size_m is not None:
        if abs(meta.pixel_size_m - 9000) > 100:
            issues.append(f"Pixel size {meta.pixel_size_m}m != expected 9000m")

    # dtype check
    if meta.dtype and meta.dtype != "float32":
        issues.append(f"dtype={meta.dtype}, expected float32")

    # Domain check
    if meta.value_min is not None and meta.value_max is not None:
        if meta.product == "condition":
            if meta.value_min < 0.9 or meta.value_max > 5.1:
                issues.append(f"Condition domain [{meta.value_min:.2f}, {meta.value_max:.2f}] outside [1,5]")
        elif meta.product == "progress":
            if meta.value_min < -0.01 or meta.value_max > 1.01:
                issues.append(f"Progress domain [{meta.value_min:.2f}, {meta.value_max:.2f}] outside [0,1]")

    passed = len(issues) == 0
    notes = "; ".join(issues) if issues else ""

    return passed, notes


# =============================================================================
# MANIFEST BUILDER
# =============================================================================

def find_all_tifs(archive_root: Path, year: Optional[int] = None,
                  crop: Optional[str] = None) -> List[Path]:
    """Find all extracted GeoTIFF files in the archive."""
    tifs = []
    for p in archive_root.rglob("*.tif"):
        if "_manifests" in str(p) or "output" in str(p):
            continue

        # Filter by year if specified
        if year is not None:
            parsed = parse_tif_filename(p.name)
            if parsed and parsed["year"] != year:
                continue

        # Filter by crop if specified
        if crop is not None:
            parsed = parse_tif_filename(p.name)
            canonical_crop = CROP_MAP.get(crop.lower(), crop.lower())
            if parsed and parsed["crop"] != canonical_crop:
                continue

        tifs.append(p)

    return sorted(tifs)


def build_manifest(archive_root: Path, year: Optional[int] = None,
                   crop: Optional[str] = None,
                   skip_stats: bool = False) -> Tuple[List[TifMetadata], List[RegionStat]]:
    """
    Build complete file manifest and region stats for all extracted TIFs.

    Returns (manifest_rows, region_stats).
    """
    tifs = find_all_tifs(archive_root, year=year, crop=crop)
    logger.info(f"Found {len(tifs)} TIF files to manifest")

    if not RASTERIO_AVAILABLE:
        logger.warning("rasterio not available — skipping raster metadata and stats")

    manifest = []
    all_region_stats = []

    for i, tif_path in enumerate(tifs):
        if (i + 1) % 50 == 0 or i == 0:
            logger.info(f"  Processing {i+1}/{len(tifs)}: {tif_path.name}")

        # Parse filename
        parsed = parse_tif_filename(tif_path.name)
        if not parsed:
            logger.warning(f"  Skipping unparseable filename: {tif_path.name}")
            continue

        # File metadata
        stat = tif_path.stat()
        file_hash = sha256_file(tif_path)

        meta = TifMetadata(
            file_path=str(tif_path),
            file_name=tif_path.name,
            file_sha256=file_hash,
            file_bytes=stat.st_size,
            modified_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            crop=parsed["crop"],
            product=parsed["product"],
            year=parsed["year"],
            nass_week=parsed["nass_week"],
            week_ending_date=parsed["week_ending_date"],
            series_id=parsed["series_id"],
        )

        # Raster metadata
        if RASTERIO_AVAILABLE:
            raster_meta = read_raster_metadata(tif_path)
            meta.crs_wkt = raster_meta.get("crs_wkt")
            meta.pixel_size_m = raster_meta.get("pixel_size_m")
            meta.dtype = raster_meta.get("dtype")
            meta.nodata_value = raster_meta.get("nodata_value")
            meta.width = raster_meta.get("width")
            meta.height = raster_meta.get("height")
            meta.bbox_xmin = raster_meta.get("bbox_xmin")
            meta.bbox_ymin = raster_meta.get("bbox_ymin")
            meta.bbox_xmax = raster_meta.get("bbox_xmax")
            meta.bbox_ymax = raster_meta.get("bbox_ymax")
            meta.value_min = raster_meta.get("value_min")
            meta.value_max = raster_meta.get("value_max")
            meta.value_mean = raster_meta.get("value_mean")
            meta.pct_nodata = raster_meta.get("pct_nodata")

            # QA checks
            passed, notes = run_qa_checks(meta)
            meta.qa_passed = passed
            meta.qa_notes = notes
            if not passed:
                logger.warning(f"  QA FAIL {tif_path.name}: {notes}")

            # Region stats
            if not skip_stats:
                national_stats = compute_national_stats(tif_path)
                for s in national_stats:
                    rs = RegionStat(
                        series_id=meta.series_id,
                        year=meta.year,
                        nass_week=meta.nass_week,
                        week_ending_date=meta.week_ending_date,
                        region_id=s["region_id"],
                        region_type=s["region_type"],
                        stat_name=s["stat_name"],
                        value=s["value"],
                        pixel_count=s.get("pixel_count"),
                    )
                    all_region_stats.append(rs)

        manifest.append(meta)

    logger.info(f"Manifest complete: {len(manifest)} files, {len(all_region_stats)} stat rows")

    # QA summary
    qa_pass = sum(1 for m in manifest if m.qa_passed)
    qa_fail = sum(1 for m in manifest if not m.qa_passed)
    if qa_fail > 0:
        logger.warning(f"QA Summary: {qa_pass} passed, {qa_fail} failed")
    else:
        logger.info(f"QA Summary: {qa_pass} passed, 0 failed")

    return manifest, all_region_stats


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_db_connection():
    """Get PostgreSQL connection using environment variables."""
    if not PSYCOPG2_AVAILABLE:
        raise ImportError("psycopg2 required for database operations")

    from dotenv import load_dotenv
    # us -> collectors -> agents -> src -> RLC-Agent
    project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
    load_dotenv(project_root / ".env")

    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )

    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )


def save_to_database(manifest: List[TifMetadata], region_stats: List[RegionStat],
                     conn=None, run_id: Optional[str] = None,
                     years_processed: str = "", crops_processed: str = "",
                     duration_sec: Optional[float] = None) -> Dict:
    """
    Save manifest and region stats to PostgreSQL bronze layer.

    Returns summary dict with insert counts.
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    if run_id is None:
        run_id = str(uuid.uuid4())

    cur = conn.cursor()
    files_added = 0
    files_updated = 0
    stats_added = 0
    qa_failures = sum(1 for m in manifest if not m.qa_passed)

    try:
        # Record ingest run
        cur.execute("""
            INSERT INTO bronze.cpc_ingest_run
                (ingest_run_id, collector_version, source, years_processed,
                 crops_processed, duration_sec)
            VALUES (%s, %s, 'local', %s, %s, %s)
        """, (run_id, COLLECTOR_VERSION, years_processed, crops_processed, duration_sec))

        # Upsert file manifest
        for meta in manifest:
            cur.execute("""
                INSERT INTO bronze.cpc_file_manifest
                    (file_sha256, series_id, ingest_run_id,
                     year, nass_week, week_ending_date,
                     file_path, file_name, file_bytes, modified_utc,
                     crs_wkt, pixel_size_m, dtype, nodata_value,
                     width, height,
                     bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                     value_min, value_max, value_mean, pct_nodata,
                     qa_passed, qa_notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id, year, nass_week)
                DO UPDATE SET
                    file_sha256 = EXCLUDED.file_sha256,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    file_path = EXCLUDED.file_path,
                    file_bytes = EXCLUDED.file_bytes,
                    modified_utc = EXCLUDED.modified_utc,
                    value_min = EXCLUDED.value_min,
                    value_max = EXCLUDED.value_max,
                    value_mean = EXCLUDED.value_mean,
                    pct_nodata = EXCLUDED.pct_nodata,
                    qa_passed = EXCLUDED.qa_passed,
                    qa_notes = EXCLUDED.qa_notes,
                    collected_at = NOW()
            """, (
                meta.file_sha256, meta.series_id, run_id,
                meta.year, meta.nass_week, meta.week_ending_date,
                meta.file_path, meta.file_name, meta.file_bytes, meta.modified_utc,
                meta.crs_wkt, meta.pixel_size_m, meta.dtype, meta.nodata_value,
                meta.width, meta.height,
                meta.bbox_xmin, meta.bbox_ymin, meta.bbox_xmax, meta.bbox_ymax,
                meta.value_min, meta.value_max, meta.value_mean, meta.pct_nodata,
                meta.qa_passed, meta.qa_notes,
            ))

            # Check if insert or update
            if cur.statusmessage and "INSERT" in cur.statusmessage:
                files_added += 1
            else:
                files_updated += 1

        # Upsert region stats
        for rs in region_stats:
            cur.execute("""
                INSERT INTO bronze.cpc_region_stats
                    (series_id, year, nass_week, week_ending_date,
                     region_id, region_type, stat_name, value, pixel_count,
                     ingest_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id, year, nass_week, region_id, stat_name)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    pixel_count = EXCLUDED.pixel_count,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    collected_at = NOW()
            """, (
                rs.series_id, rs.year, rs.nass_week, rs.week_ending_date,
                rs.region_id, rs.region_type, rs.stat_name, rs.value,
                rs.pixel_count, run_id,
            ))
            stats_added += 1

        # Update ingest run with counts
        cur.execute("""
            UPDATE bronze.cpc_ingest_run
            SET files_added = %s, files_updated = %s, qa_failures = %s
            WHERE ingest_run_id = %s
        """, (files_added, files_updated, qa_failures, run_id))

        conn.commit()
        logger.info(f"Database save complete: {files_added} added, {files_updated} updated, "
                     f"{stats_added} stat rows, {qa_failures} QA failures")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cur.close()
        if close_conn:
            conn.close()

    return {
        "ingest_run_id": run_id,
        "files_added": files_added,
        "files_updated": files_updated,
        "stats_rows": stats_added,
        "qa_failures": qa_failures,
    }


# =============================================================================
# JSON EXPORT
# =============================================================================

def save_manifest_json(manifest: List[TifMetadata], region_stats: List[RegionStat],
                       output_path: Path):
    """Save manifest and region stats to JSON file."""
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collector_version": COLLECTOR_VERSION,
        "file_count": len(manifest),
        "stat_count": len(region_stats),
        "manifest": [asdict(m) for m in manifest],
        "region_stats": [asdict(rs) for rs in region_stats],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    logger.info(f"Saved manifest JSON: {output_path} ({len(manifest)} files, {len(region_stats)} stats)")


# =============================================================================
# QA REPORT
# =============================================================================

def print_qa_report(manifest: List[TifMetadata]):
    """Print a formatted QA report to stdout."""
    total = len(manifest)
    passed = sum(1 for m in manifest if m.qa_passed)
    failed = sum(1 for m in manifest if not m.qa_passed)
    no_raster = sum(1 for m in manifest if m.crs_wkt is None)

    print(f"\n{'='*60}")
    print(f"  CPC Gridded Layers QA Report")
    print(f"{'='*60}")
    print(f"  Total files:      {total}")
    print(f"  QA Passed:        {passed}")
    print(f"  QA Failed:        {failed}")
    print(f"  No raster data:   {no_raster}")
    print()

    # Breakdown by crop/product
    by_series = {}
    for m in manifest:
        key = f"{m.crop} {m.product}"
        if key not in by_series:
            by_series[key] = {"total": 0, "passed": 0, "failed": 0}
        by_series[key]["total"] += 1
        if m.qa_passed:
            by_series[key]["passed"] += 1
        else:
            by_series[key]["failed"] += 1

    print(f"  {'Series':<30} {'Total':>6} {'Pass':>6} {'Fail':>6}")
    print(f"  {'-'*30} {'-'*6} {'-'*6} {'-'*6}")
    for series, counts in sorted(by_series.items()):
        print(f"  {series:<30} {counts['total']:>6} {counts['passed']:>6} {counts['failed']:>6}")

    # List failures
    if failed > 0:
        print(f"\n  Failed files:")
        for m in manifest:
            if not m.qa_passed:
                print(f"    {m.file_name}: {m.qa_notes}")

    # Year coverage
    years = sorted(set(m.year for m in manifest))
    if years:
        print(f"\n  Year coverage: {years[0]}–{years[-1]}")
        for yr in years:
            yr_files = [m for m in manifest if m.year == yr]
            crops = sorted(set(m.crop for m in yr_files))
            weeks = sorted(set(m.nass_week for m in yr_files))
            week_range = f"w{min(weeks)}-w{max(weeks)}" if weeks else "none"
            print(f"    {yr}: {len(yr_files)} files, crops={crops}, weeks={week_range}")

    print(f"{'='*60}\n")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="NASS Crop Progress & Condition Gridded Layers Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  extract    Extract ZIP files to TIF directories
  manifest   Build file manifest + raster metadata
  ingest     Full pipeline: extract -> manifest -> QA -> save to DB
  qa-report  Run QA checks and print report

Examples:
  python nass_cpc_gridded_collector.py extract --year 2025
  python nass_cpc_gridded_collector.py manifest --year 2025 --output-json output/manifest.json
  python nass_cpc_gridded_collector.py ingest --save-db
  python nass_cpc_gridded_collector.py qa-report --year 2024
        """
    )

    parser.add_argument("command", choices=["extract", "manifest", "ingest", "qa-report"],
                        help="Operation to perform")
    parser.add_argument("--year", type=int, help="Process only a specific year")
    parser.add_argument("--crop", type=str, help="Process only a specific crop (corn, soy, cotton, wheat)")
    parser.add_argument("--force", action="store_true", help="Force re-extraction / re-processing")
    parser.add_argument("--output-json", type=str, help="Save manifest to JSON file")
    parser.add_argument("--save-db", action="store_true", help="Save to PostgreSQL bronze layer")
    parser.add_argument("--skip-stats", action="store_true", help="Skip region statistics computation")
    parser.add_argument("--archive-root", type=str, default=str(ARCHIVE_ROOT),
                        help="Root directory of the CPC archive")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    archive_root = Path(args.archive_root)
    if not archive_root.exists():
        logger.error(f"Archive root not found: {archive_root}")
        return

    start_time = datetime.now()

    # ---- EXTRACT ----
    if args.command == "extract":
        summary = extract_all_zips(archive_root, year=args.year, crop=args.crop, force=args.force)
        print(f"\nExtraction Summary:")
        print(f"  ZIPs found:     {summary['zips_found']}")
        print(f"  TIFs extracted: {summary['tifs_extracted']}")
        print(f"  ZIPs skipped:   {summary['zips_skipped']}")

    # ---- MANIFEST ----
    elif args.command == "manifest":
        if not RASTERIO_AVAILABLE:
            logger.warning("rasterio not installed — manifest will have filename metadata only")

        manifest, region_stats = build_manifest(
            archive_root, year=args.year, crop=args.crop, skip_stats=args.skip_stats
        )

        if args.output_json:
            save_manifest_json(manifest, region_stats, Path(args.output_json))

        print(f"\nManifest Summary:")
        print(f"  Files:       {len(manifest)}")
        print(f"  Stat rows:   {len(region_stats)}")
        qa_fail = sum(1 for m in manifest if not m.qa_passed)
        print(f"  QA failures: {qa_fail}")

    # ---- INGEST ----
    elif args.command == "ingest":
        # Step 1: Extract
        logger.info("Step 1/3: Extracting ZIPs...")
        extract_summary = extract_all_zips(archive_root, year=args.year, crop=args.crop, force=args.force)

        # Step 2: Build manifest
        logger.info("Step 2/3: Building manifest...")
        manifest, region_stats = build_manifest(
            archive_root, year=args.year, crop=args.crop, skip_stats=args.skip_stats
        )

        duration = (datetime.now() - start_time).total_seconds()

        # Save JSON if requested
        if args.output_json:
            save_manifest_json(manifest, region_stats, Path(args.output_json))

        # Step 3: Save to DB
        if args.save_db:
            logger.info("Step 3/3: Saving to database...")
            years_str = str(args.year) if args.year else "all"
            crops_str = args.crop if args.crop else "all"
            db_summary = save_to_database(
                manifest, region_stats,
                years_processed=years_str,
                crops_processed=crops_str,
                duration_sec=duration,
            )
            print(f"\nIngest Summary:")
            print(f"  Run ID:        {db_summary['ingest_run_id']}")
            print(f"  Files added:   {db_summary['files_added']}")
            print(f"  Files updated: {db_summary['files_updated']}")
            print(f"  Stats rows:    {db_summary['stats_rows']}")
            print(f"  QA failures:   {db_summary['qa_failures']}")
            print(f"  Duration:      {duration:.1f}s")
        else:
            print(f"\nIngest Summary (dry run — use --save-db to persist):")
            print(f"  Files:       {len(manifest)}")
            print(f"  Stat rows:   {len(region_stats)}")
            print(f"  Duration:    {duration:.1f}s")

    # ---- QA REPORT ----
    elif args.command == "qa-report":
        if not RASTERIO_AVAILABLE:
            print("ERROR: rasterio required for QA report")
            return

        manifest, _ = build_manifest(
            archive_root, year=args.year, crop=args.crop, skip_stats=True
        )
        print_qa_report(manifest)


if __name__ == "__main__":
    main()
