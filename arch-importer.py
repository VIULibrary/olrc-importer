import subprocess
import logging
from pathlib import Path
from tqdm import tqdm
import sys
import csv
import os
from datetime import datetime
import time

#------
#fixes for segment errors
#------

#SOURCE IT ! 

# Config
AIP_ROOT = Path("/Volumes/Vintage-1/archCoppul/Under4gb/")
CONTAINER = "viurrspace-core-pre-may-05-24"
SEGMENT_CONTAINER = f"{CONTAINER}_segments"
SEGMENT_SIZE = 5 * 1024 * 1024 * 1024 - 100 * 1024 * 1024  # 4.9GB (safe margin)
LOGFILE = Path(__file__).parent / "arch-logs" / f"{AIP_ROOT.name}-log.txt"
CSV_SUMMARY = Path(__file__).parent / "arch-logs" / f"{AIP_ROOT.name}-upload-summary.csv"
MAX_RETRIES = 3
TIMEOUT = 36000  # 10 hours

logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
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
            error[:500]
        ])

def get_file_size(path):
    """Get file size in human readable format"""
    size = path.stat().st_size
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"

def check_credentials():
    """Verify OpenStack credentials"""
    try:
        subprocess.run(
            ["python3", "-m", "swiftclient.shell", "auth"],
            capture_output=True,
            check=True,
            env=os.environ,
            timeout=30
        )
        print("✅ Credentials verified")
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        print("❌ Authentication failed")
        return False

def test_connection():
    """Test basic OpenStack connectivity"""
    try:
        result = subprocess.run(
            ["python3", "-m", "swiftclient.shell", "list"],
            capture_output=True,
            timeout=30
        )
        if result.returncode == 0:
            print("✅ Connection test passed")
            return True
        return False
    except subprocess.TimeoutExpired:
        print("❌ Connection timed out")
        return False

def ensure_container_exists():
    """Create containers if they don't exist"""
    for name in [CONTAINER, SEGMENT_CONTAINER]:
        try:
            subprocess.run(
                ["python3", "-m", "swiftclient.shell", "stat", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=30
            )
        except subprocess.CalledProcessError:
            try:
                subprocess.run(
                    ["python3", "-m", "swiftclient.shell", "post", name],
                    check=True,
                    timeout=30
                )
                print(f"✅ Created container: {name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to create container: {name}")
                return False
    return True

def cleanup_segments(filename):
    """Clean up orphaned segments for a specific file"""
    try:
        result = subprocess.run(
            ["python3", "-m", "swiftclient.shell", "list", SEGMENT_CONTAINER],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            segments = [line for line in result.stdout.split('\n') if line and filename in line]
            if segments:
                print(f"   🧹 Cleaning up {len(segments)} orphaned segments...")
                for segment in segments:
                    subprocess.run(
                        ["python3", "-m", "swiftclient.shell", "delete", SEGMENT_CONTAINER, segment],
                        capture_output=True, timeout=30
                    )
                return len(segments)
    except Exception as e:
        print(f"   ⚠️  Could not cleanup segments: {e}")
    return 0

def upload_aip(aip_file: Path, attempt=1):
    """Upload a single AIP file with proper 5GB segment handling"""
    filename = aip_file.name
    file_size_str = get_file_size(aip_file)
    size_mb = aip_file.stat().st_size / (1024 * 1024)
    file_size_bytes = aip_file.stat().st_size

    print(f"⬆️  {filename[:45]:45} {file_size_str:>8}...", end="", flush=True)
    
    try:
        # Build command based on file size
        cmd = [
            "python3", "-m", "swiftclient.shell",
            "upload", CONTAINER, str(aip_file), "--object-name", filename
        ]
        
        # Use segmentation only for files > 5GB
        if file_size_bytes > 5 * 1024 * 1024 * 1024:
            cmd.extend([
                "--segment-size", str(SEGMENT_SIZE),
                "--segment-container", SEGMENT_CONTAINER
            ])
            segments_needed = (file_size_bytes + SEGMENT_SIZE - 1) // SEGMENT_SIZE
            print(f"\n   🔧 Segmented: {segments_needed} segments of {get_file_size_from_bytes(SEGMENT_SIZE)}", end="")

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ ({elapsed:.1f}s)")
            log_to_csv(filename, size_mb, "Success", attempt)
            return True
        else:
            print("❌")
            error_msg = result.stderr.strip() or f"Exit code: {result.returncode}"
            
            # Enhanced error reporting for segmentation issues
            if "segment" in error_msg.lower() or "upload" in error_msg.lower():
                print(f"   🔍 Error details: {error_msg[:200]}...")
            
            # Clean up any orphaned segments
            if file_size_bytes > 5 * 1024 * 1024 * 1024:
                cleaned_count = cleanup_segments(filename)
                if cleaned_count > 0:
                    print(f"   🧹 Cleaned {cleaned_count} orphaned segments")
            
            if attempt < MAX_RETRIES:
                print(f"   ↻ Retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
                return upload_aip(aip_file, attempt + 1)
            else:
                log_to_csv(filename, size_mb, "Failed", attempt, error_msg)
                return False

    except subprocess.TimeoutExpired:
        print("⏰")
        # Clean up segments on timeout
        if file_size_bytes > 5 * 1024 * 1024 * 1024:
            cleanup_segments(filename)
            
        if attempt < MAX_RETRIES:
            print(f"   ↻ Retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
            return upload_aip(aip_file, attempt + 1)
        else:
            log_to_csv(filename, size_mb, "Failed", attempt, "Timeout")
            return False
    except Exception as e:
        print(f"💥 {e}")
        if attempt < MAX_RETRIES:
            print(f"   ↻ Retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
            return upload_aip(aip_file, attempt + 1)
        else:
            log_to_csv(filename, size_mb, "Failed", attempt, str(e))
            return False

def get_file_size_from_bytes(size_bytes):
    """Get file size in human readable format from bytes"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def main():
    """Main execution function"""
    print("🚀 OpenStack Upload with 5GB Segment Limit")
    print("===========================================")
    print(f"📁 Source: {AIP_ROOT}")
    
    if not AIP_ROOT.exists() or not AIP_ROOT.is_dir():
        print("❌ Invalid directory")
        sys.exit(1)

    # Find .7z files
    aip_files = sorted(AIP_ROOT.glob("*.7z"))
    print(f"📦 Found {len(aip_files)} files to upload")
    
    if not aip_files:
        print("❌ No .7z files found")
        sys.exit(1)

    # Show upload strategy for each file
    print("\n📊 Upload strategy:")
    for f in aip_files:
        size_bytes = f.stat().st_size
        size_str = get_file_size(f)
        if size_bytes > 5 * 1024 * 1024 * 1024:
            segments = (size_bytes + SEGMENT_SIZE - 1) // SEGMENT_SIZE
            strategy = f"Segmented ({segments} segments)"
        else:
            strategy = "Direct upload"
        print(f"   {f.name} ({size_str}) - {strategy}")

    # Setup
    print("\n🔐 Checking credentials...")
    if not check_credentials():
        sys.exit(1)
    
    print("🌐 Testing connection...")
    if not test_connection():
        sys.exit(1)
    
    print("📊 Ensuring containers...")
    if not ensure_container_exists():
        sys.exit(1)
    
    init_csv()

    # Upload files
    print(f"\n🎯 Starting upload of {len(aip_files)} files...")
    
    success_count = 0
    
    for aip_file in aip_files:
        if upload_aip(aip_file):
            success_count += 1

    # Summary
    print(f"\n📊 Upload complete!")
    print(f"✅ Successful: {success_count}/{len(aip_files)}")
    print(f"❌ Failed: {len(aip_files) - success_count}/{len(aip_files)}")

if __name__ == "__main__":
    main()