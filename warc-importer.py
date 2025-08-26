import subprocess
import logging
from pathlib import Path
from tqdm import tqdm
import sys
import csv
import os
from datetime import datetime

# Config
AIP_ROOT = Path("/Volumes/Backup Plus/WARC_files/WARCS_202507_Cumulative/WARCS_23169_20250709")
CONTAINER = "warcs-cumulative"
SEGMENT_CONTAINER = f"{CONTAINER}_segments"
#SEGMENT_SIZE = 1024 * 1024 * 1024  # 1G
SEGMENT_SIZE = 4 * 1024 * 1024 * 1024 + 500 * 1024 * 1024  # 4.5GB
LOGFILE = Path(__file__).parent / "warc-logs" / f"{AIP_ROOT.name}-log.txt"
CSV_SUMMARY = Path(__file__).parent / "warc-logs" / f"{AIP_ROOT.name}-upload-summary.csv"
MAX_RETRIES = 3
TIMEOUT = 14400  # 4 hours (for 50GB+ files)
MAX_RETRIES = 5  

# Enhanced logging setup
logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # Append mode
)
logger = logging.getLogger()

def init_csv():
    """Initialize CSV log file with headers"""
    if not Path(CSV_SUMMARY).exists():
        with open(CSV_SUMMARY, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Filename", "Size (MB)", "Status", "Timestamp", "Attempts", "Error"])

def log_to_csv(filename, size_mb, status, attempts=1, error=""):
    """Log upload results to CSV"""
    with open(CSV_SUMMARY, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            filename,
            f"{size_mb:.2f}",
            status,
            datetime.now().isoformat(),
            attempts,
            error[:200]  
        ])

def check_credentials():
    """Verify OpenStack credentials"""
    required_vars = [
        "OS_AUTH_URL", "OS_PROJECT_ID", "OS_PROJECT_NAME",
        "OS_USERNAME", "OS_PASSWORD", "OS_REGION_NAME",
        "OS_USER_DOMAIN_NAME", "OS_IDENTITY_API_VERSION"
    ]
    
    missing = [var for var in required_vars if var not in os.environ]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        sys.exit(1)
    
    try:
        auth_output = subprocess.run(
            ["python3", "-m", "swiftclient.shell", "auth"],
            capture_output=True,
            text=True,
            check=True,
            env=os.environ
        )
        if "OS_AUTH_TOKEN=" not in auth_output.stdout:
            logger.error("Authentication failed - no token received")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Auth failed: {e.stderr}")
        sys.exit(1)

def ensure_container_exists():
    """Create containers if they don't exist"""
    for name in [CONTAINER, SEGMENT_CONTAINER]:
        try:
            subprocess.run(
                ["python3", "-m", "swiftclient.shell", "stat", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True
            )
        except subprocess.CalledProcessError:
            logger.info(f"Creating container {name}")
            try:
                subprocess.run(
                    ["python3", "-m", "swiftclient.shell", "post", name],
                    check=True
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to create container {name}: {e.stderr}")
                continue

def upload_aip(aip_file: Path, attempt=1):
    """Upload a single AIP file with retry logic"""
    size_mb = aip_file.stat().st_size / (1024 * 1024)
    filename = aip_file.name  # Just the filename, no path
    
    # Handle files directly in AIP_ROOT and in subdirectories
    if aip_file.parent == AIP_ROOT:
        object_name = f"{AIP_ROOT.name}/{aip_file.name}"  # File directly in AIP_ROOT
    else:
        relative_path = aip_file.relative_to(AIP_ROOT)
        object_name = f"{AIP_ROOT.name}/{relative_path}"  # File in subdirectory

    # Debugging: Log the file being processed and the calculated object name
    logger.info(f"Processing file: {aip_file}")
    print(f"Processing file: {aip_file}")
    logger.info(f"Calculated object name: {object_name}")
    print(f"Calculated object name: {object_name}")

    try:
        # Upload with directory structure preserved
        result = subprocess.run(
            [
                "python3", "-m", "swiftclient.shell",
                "upload",
                "--segment-size", str(SEGMENT_SIZE),
                "--segment-container", SEGMENT_CONTAINER,
                CONTAINER,
                str(aip_file),  # Full path for source
                "--object-name", str(object_name)  # Preserve directory structure
            ],
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info(f"Uploaded: {object_name}")
        print(f"✔ Uploaded {object_name}")
        log_to_csv(filename, size_mb, "Success", attempt)
        return True

    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip()
        logger.error(f"Failed {object_name} (attempt {attempt}): {error_msg}")
        
        if attempt < MAX_RETRIES:
            print(f"↻ Retrying {object_name} (attempt {attempt + 1}/{MAX_RETRIES})...")
            return upload_aip(aip_file, attempt + 1)
        else:
            print(f"✗ Failed {object_name} after {MAX_RETRIES} attempts")
            log_to_csv(filename, size_mb, "Failed", attempt, error_msg)
            return False

def main():
    """Main execution function"""
    print("🔐 Checking credentials...")
    check_credentials()
    
    print("📦 Ensuring containers exist...")
    ensure_container_exists()
    
    print("📝 Initializing logs...")
    init_csv()

    # Check if the path exists first
    if not AIP_ROOT.exists():
        print(f"❌ Error: Path does not exist: {AIP_ROOT}")
        logger.error(f"Path does not exist: {AIP_ROOT}")
        sys.exit(1)
    
    if not AIP_ROOT.is_dir():
        print(f"❌ Error: Path is not a directory: {AIP_ROOT}")
        logger.error(f"Path is not a directory: {AIP_ROOT}")
        sys.exit(1)

    # Get all files (including those in subdirectories) in the folder
    aip_files = sorted(AIP_ROOT.rglob("*"))
    aip_files = [f for f in aip_files if f.is_file()]  # Filter out directories
    print(f"\n🗂️ Found {len(aip_files)} files to upload (including subdirectories).\n")

    success_count = 0
    for aip_file in tqdm(aip_files, desc="Uploading AIPs", unit="file"):
        if upload_aip(aip_file):
            success_count += 1

    print(f"\n✅ Successfully uploaded {success_count}/{len(aip_files)} files")
    logger.info(f"Upload summary: {success_count} succeeded, {len(aip_files) - success_count} failed")

if __name__ == "__main__":
    main()