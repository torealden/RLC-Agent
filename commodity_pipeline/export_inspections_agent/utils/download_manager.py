"""
FGIS Download Manager
Handles downloading export grain inspection CSV files from FGIS website
"""

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Result of a download operation"""
    success: bool
    file_path: Optional[Path]
    url: str
    status_code: Optional[int]
    file_size: int = 0
    download_time: float = 0.0
    error_message: Optional[str] = None
    md5_hash: Optional[str] = None
    is_new_data: bool = True  # False if file unchanged from cache
    
    def __str__(self):
        if self.success:
            return f"Downloaded {self.file_path.name} ({self.file_size:,} bytes) in {self.download_time:.1f}s"
        return f"Failed to download {self.url}: {self.error_message}"


class FGISDownloadManager:
    """
    Manages downloading of FGIS export grain inspection data files
    """
    
    # FGIS Export Grain Report base URL
    BASE_URL = "https://fgisonline.ams.usda.gov/exportgrainreport/"
    
    # Alternative URLs to try if primary fails
    ALTERNATIVE_URLS = [
        "https://fgisonline.ams.usda.gov/ExportGrainReport/",
        "https://www.ams.usda.gov/services/fgis/export-grain-inspection",
    ]
    
    # File naming patterns
    FILE_PATTERNS = [
        "CY{year}.csv",           # Primary pattern: CY2025.csv
        "CY{year}.CSV",           # Uppercase variant
        "cy{year}.csv",           # Lowercase variant
        "{year}.csv",             # Just year
    ]
    
    def __init__(self, 
                 data_directory: Path = None,
                 timeout: int = 300,
                 retry_attempts: int = 3,
                 retry_delay: int = 10):
        """
        Initialize download manager
        
        Args:
            data_directory: Where to save downloaded files
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts on failure
            retry_delay: Delay between retries in seconds
        """
        self.data_directory = Path(data_directory or "./data/raw")
        self.data_directory.mkdir(parents=True, exist_ok=True)
        
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Create session with retry logic
        self.session = self._create_session()
        
        # Cache of file hashes for change detection
        self._file_hashes: Dict[str, str] = {}
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers to mimic browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/csv,text/plain,application/csv,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        return session
    
    def _get_file_hash(self, content: bytes) -> str:
        """Calculate MD5 hash of file content"""
        return hashlib.md5(content).hexdigest()
    
    def _check_file_changed(self, url: str, new_hash: str) -> bool:
        """Check if file content has changed since last download"""
        cached_hash = self._file_hashes.get(url)
        if cached_hash is None:
            return True  # No cached hash, consider it new
        return cached_hash != new_hash
    
    def _try_download_url(self, url: str) -> Tuple[bool, Optional[bytes], Optional[int], Optional[str]]:
        """
        Attempt to download from a single URL
        
        Returns:
            Tuple of (success, content, status_code, error_message)
        """
        try:
            logger.debug(f"Attempting download from: {url}")
            
            response = self.session.get(
                url,
                timeout=self.timeout,
                stream=True
            )
            
            if response.status_code == 200:
                # Check content type
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text' in content_type or 'csv' in content_type or 'octet-stream' in content_type:
                    content = response.content
                    return True, content, 200, None
                else:
                    return False, None, response.status_code, f"Unexpected content type: {content_type}"
            
            elif response.status_code == 404:
                return False, None, 404, "File not found"
            
            else:
                return False, None, response.status_code, f"HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            return False, None, None, "Request timed out"
        except requests.exceptions.ConnectionError as e:
            return False, None, None, f"Connection error: {str(e)}"
        except Exception as e:
            return False, None, None, f"Unexpected error: {str(e)}"
    
    def download_year(self, year: int, force: bool = False) -> DownloadResult:
        """
        Download CSV file for a specific year
        
        Args:
            year: Calendar year to download (e.g., 2025)
            force: Force download even if file exists and is unchanged
            
        Returns:
            DownloadResult with status and file path
        """
        start_time = time.time()
        
        logger.info(f"Downloading export inspections data for year {year}")
        
        # Try each file pattern
        for pattern in self.FILE_PATTERNS:
            filename = pattern.format(year=year)
            
            # Try primary URL
            url = urljoin(self.BASE_URL, filename)
            success, content, status_code, error = self._try_download_url(url)
            
            if success:
                break
            
            # Try alternative URLs if primary fails
            if status_code == 404:
                for alt_base in self.ALTERNATIVE_URLS:
                    alt_url = urljoin(alt_base, filename)
                    success, content, status_code, error = self._try_download_url(alt_url)
                    if success:
                        url = alt_url
                        break
                
                if success:
                    break
        
        if not success:
            return DownloadResult(
                success=False,
                file_path=None,
                url=url,
                status_code=status_code,
                error_message=error,
                download_time=time.time() - start_time
            )
        
        # Calculate hash to check if content changed
        file_hash = self._get_file_hash(content)
        is_new_data = self._check_file_changed(url, file_hash)
        
        # Check if we should skip (unchanged file and not forcing)
        local_path = self.data_directory / f"CY{year}.csv"
        if not force and not is_new_data and local_path.exists():
            logger.info(f"File unchanged for year {year}, skipping save")
            return DownloadResult(
                success=True,
                file_path=local_path,
                url=url,
                status_code=200,
                file_size=len(content),
                download_time=time.time() - start_time,
                md5_hash=file_hash,
                is_new_data=False
            )
        
        # Save file
        try:
            local_path.write_bytes(content)
            self._file_hashes[url] = file_hash
            
            logger.info(f"Successfully downloaded {filename} ({len(content):,} bytes)")
            
            return DownloadResult(
                success=True,
                file_path=local_path,
                url=url,
                status_code=200,
                file_size=len(content),
                download_time=time.time() - start_time,
                md5_hash=file_hash,
                is_new_data=True
            )
            
        except IOError as e:
            return DownloadResult(
                success=False,
                file_path=None,
                url=url,
                status_code=200,
                error_message=f"Failed to save file: {str(e)}",
                download_time=time.time() - start_time
            )
    
    def download_years(self, start_year: int, end_year: int = None,
                       force: bool = False) -> List[DownloadResult]:
        """
        Download CSV files for a range of years
        
        Args:
            start_year: First year to download
            end_year: Last year to download (defaults to current year)
            force: Force download even if files exist
            
        Returns:
            List of DownloadResult objects
        """
        if end_year is None:
            end_year = datetime.now().year
        
        results = []
        
        for year in range(start_year, end_year + 1):
            result = self.download_year(year, force=force)
            results.append(result)
            
            # Add small delay between requests to be polite
            if year < end_year:
                time.sleep(1)
        
        # Summary logging
        successful = sum(1 for r in results if r.success)
        new_data = sum(1 for r in results if r.success and r.is_new_data)
        logger.info(f"Downloaded {successful}/{len(results)} files, {new_data} with new data")
        
        return results
    
    def download_current_year(self, force: bool = False) -> DownloadResult:
        """Download data for the current year"""
        return self.download_year(datetime.now().year, force=force)
    
    def check_for_updates(self, year: int = None) -> bool:
        """
        Check if new data is available without downloading
        
        Uses HEAD request to check Last-Modified or ETag headers
        
        Args:
            year: Year to check (defaults to current year)
            
        Returns:
            True if new data may be available
        """
        year = year or datetime.now().year
        
        for pattern in self.FILE_PATTERNS:
            filename = pattern.format(year=year)
            url = urljoin(self.BASE_URL, filename)
            
            try:
                response = self.session.head(url, timeout=30)
                
                if response.status_code == 200:
                    # Check for modification indicators
                    etag = response.headers.get('ETag')
                    last_modified = response.headers.get('Last-Modified')
                    content_length = response.headers.get('Content-Length')
                    
                    logger.debug(f"File check: ETag={etag}, Last-Modified={last_modified}, "
                               f"Content-Length={content_length}")
                    
                    # If we have cached info, compare
                    # For now, just return True if file exists
                    return True
                    
            except Exception as e:
                logger.warning(f"Error checking for updates: {e}")
                continue
        
        return False
    
    def get_available_years(self) -> List[int]:
        """
        Get list of years with available data
        
        Returns:
            List of years that have downloadable files
        """
        available = []
        current_year = datetime.now().year
        
        # Check years from 2010 to current
        for year in range(2010, current_year + 1):
            for pattern in self.FILE_PATTERNS:
                filename = pattern.format(year=year)
                url = urljoin(self.BASE_URL, filename)
                
                try:
                    response = self.session.head(url, timeout=10)
                    if response.status_code == 200:
                        available.append(year)
                        break
                except:
                    continue
        
        logger.info(f"Found data available for years: {available}")
        return available
    
    def get_local_files(self) -> Dict[int, Path]:
        """
        Get dictionary of locally available files
        
        Returns:
            Dict mapping year to file path
        """
        files = {}
        
        for path in self.data_directory.glob("CY*.csv"):
            try:
                year = int(path.stem.replace("CY", "").replace("cy", ""))
                files[year] = path
            except ValueError:
                continue
        
        return files
    
    def cleanup_old_files(self, keep_years: int = 5) -> List[Path]:
        """
        Remove old downloaded files
        
        Args:
            keep_years: Number of recent years to keep
            
        Returns:
            List of deleted file paths
        """
        current_year = datetime.now().year
        cutoff_year = current_year - keep_years
        
        deleted = []
        local_files = self.get_local_files()
        
        for year, path in local_files.items():
            if year < cutoff_year:
                try:
                    path.unlink()
                    deleted.append(path)
                    logger.info(f"Deleted old file: {path}")
                except Exception as e:
                    logger.warning(f"Failed to delete {path}: {e}")
        
        return deleted


class FGISDataSourceChecker:
    """
    Utility class to check FGIS data source availability and structure
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0'
        })
    
    def check_website_status(self) -> Dict:
        """Check if FGIS website is accessible"""
        url = "https://fgisonline.ams.usda.gov/exportgrainreport/"
        
        try:
            response = self.session.get(url, timeout=30)
            return {
                'accessible': response.status_code == 200,
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds()
            }
        except Exception as e:
            return {
                'accessible': False,
                'error': str(e)
            }
    
    def get_available_files(self) -> List[Dict]:
        """
        Try to get list of available files from the website
        Note: May require parsing HTML if no API endpoint exists
        """
        # This would need to be customized based on the actual website structure
        # For now, we check known years
        available = []
        current_year = datetime.now().year
        
        for year in range(current_year - 5, current_year + 1):
            url = f"https://fgisonline.ams.usda.gov/exportgrainreport/CY{year}.csv"
            try:
                response = self.session.head(url, timeout=10)
                if response.status_code == 200:
                    available.append({
                        'year': year,
                        'url': url,
                        'size': response.headers.get('Content-Length'),
                        'last_modified': response.headers.get('Last-Modified')
                    })
            except:
                continue
        
        return available
    
    def validate_csv_structure(self, file_path: Path) -> Dict:
        """
        Validate that a downloaded CSV has expected structure
        """
        import csv
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Count rows
                row_count = sum(1 for _ in reader)
            
            # Check for required columns
            required = ['Thursday', 'Serial No.', 'Grain', 'Destination', 'Pounds']
            headers_upper = [h.upper().strip() for h in headers]
            missing = [r for r in required if r.upper() not in headers_upper]
            
            return {
                'valid': len(missing) == 0,
                'column_count': len(headers),
                'row_count': row_count,
                'missing_columns': missing,
                'headers': headers[:10]  # First 10 for inspection
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': str(e)
            }
