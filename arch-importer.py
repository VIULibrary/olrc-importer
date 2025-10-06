import subprocess
import logging
from pathlib import Path
from tqdm import tqdm
import sys
import csv
import os
from datetime import datetime
import time
import json

# Config
AIP_ROOT = Path("/Volumes/Vintage-1/archCoppul/Under4gb/10-6-2")
CONTAINER = "viurrspace-core-pre-may-05-24"
SEGMENT_CONTAINER = f"{CONTAINER}_segments"
SEGMENT_SIZE = 4 * 1024 * 1024 * 1024 + 500 * 1024 * 1024  # 4.5GB
LOGFILE = Path(__file__).parent / "arch-logs" / f"{AIP_ROOT.name}-log.txt"
CSV_SUMMARY = Path(__file__).parent / "arch-logs" / f"{AIP_ROOT.name}-upload-summary.csv"
STATE_FILE = Path(__file__).parent / "arch-logs" / f"{AIP_ROOT.name}-upload-state.json"
MAX_RETRIES = 5
TIMEOUT = 14400  # 4 hours

# Enhanced logging setup
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
            error[:200]  
        ])

def save_state(uploaded_files, current_file=None, current_attempt=1):
    """Save upload state to resume later"""
    try:
        state = {
            'uploaded_files': list(uploaded_files),  # Convert set to list for JSON
            'current_file': str(current_file) if current_file else None,
            'current_attempt': current_attempt,
            'timestamp': datetime.now().isoformat()
        }
        # Ensure the directory exists
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"⚠️  Could not save state: {e}")

def load_state():
    """Load upload state if it exists"""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r') as f:
                content = f.read().strip()
                if not content:  # Empty file
                    print("⚠️  State file is empty, starting fresh")
                    return None
                state = json.loads(content)
                # Convert back to set
                if 'uploaded_files' in state:
                    state['uploaded_files'] = set(state['uploaded_files'])
                return state
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"⚠️  Corrupted state file: {e}. Starting fresh.")
            # Backup the corrupted file
            backup_file = STATE_FILE.with_suffix('.json.corrupted')
            try:
                STATE_FILE.rename(backup_file)
                print(f"📁 Backed up corrupted state to: {backup_file}")
            except:
                pass  # If backup fails, just continue
            return None
        except Exception as e:
            print(f"⚠️  Error reading state file: {e}. Starting fresh.")
            return None
    return None

def cleanup_segments(filename):
    """Clean up orphaned segments if upload was interrupted"""
    try:
        # List segments in segment container
        result = subprocess.run(
            ["python3", "-m", "swiftclient.shell", "list", SEGMENT_CONTAINER],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            # Look for segments belonging to this file
            segments = [line for line in result.stdout.split('\n') 
                       if line and filename in line]
            if segments:
                print(f"🧹 Cleaning up {len(segments)} orphaned segments...")
                for segment in segments:
                    subprocess.run(
                        ["python3", "-m", "swiftclient.shell", "delete", 
                         SEGMENT_CONTAINER, segment],
                        capture_output=True, timeout=30
                    )
    except Exception as e:
        print(f"⚠️  Could not cleanup segments: {e}")

def check_object_exists(filename):
    """Check if a file already exists in Swift"""
    try:
        result = subprocess.run(
            ["python3", "-m", "swiftclient.shell", "stat", CONTAINER, filename],
            capture_output=True, text=True, timeout=30
        )
        return result.returncode == 0
    except:
        return False

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
    required_vars = [
        "OS_AUTH_URL", "OS_PROJECT_ID", "OS_PROJECT_NAME",
        "OS_USERNAME", "OS_PASSWORD", "OS_REGION_NAME",
        "OS_USER_DOMAIN_NAME", "OS_IDENTITY_API_VERSION"
    ]
    
    missing = [var for var in required_vars if var not in os.environ]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        print(f"❌ Missing environment variables: {missing}")
        sys.exit(1)
    
    try:
        subprocess.run(
            ["python3", "-m", "swiftclient.shell", "auth"],
            capture_output=True,
            check=True,
            env=os.environ,
            timeout=30
        )
        print("✅ Credentials verified")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        print("❌ Authentication failed")
        sys.exit(1)

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
            except subprocess.CalledProcessError:
                print(f"❌ Failed to create container: {name}")

def upload_aip(aip_file: Path, attempt=1, uploaded_files=None):
    """Upload a single AIP file with resume support"""
    if uploaded_files is None:
        uploaded_files = set()
        
    filename = aip_file.name
    file_size_str = get_file_size(aip_file)
    size_mb = aip_file.stat().st_size / (1024 * 1024)

    # Check if already uploaded
    if filename in uploaded_files:
        print(f"⏩ {filename[:45]:45} {file_size_str:>8} (already uploaded)")
        return True
        
    if check_object_exists(filename):
        print(f"⏩ {filename[:45]:45} {file_size_str:>8} (exists in cloud)")
        uploaded_files.add(filename)
        save_state(uploaded_files)
        return True

    print(f"⬆️  {filename[:45]:45} {file_size_str:>8}...", end="", flush=True)
    
    try:
        # Clean up any orphaned segments from previous attempts
        if attempt == 1:
            cleanup_segments(filename)
            
        cmd = [
            "python3", "-m", "swiftclient.shell",
            "upload", CONTAINER, str(aip_file), "--object-name", filename
        ]
        
        if aip_file.stat().st_size > 5 * 1024 * 1024 * 1024:
            cmd.extend(["--segment-size", str(SEGMENT_SIZE), "--segment-container", SEGMENT_CONTAINER])

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ ({elapsed:.1f}s)")
            uploaded_files.add(filename)
            save_state(uploaded_files)
            log_to_csv(filename, size_mb, "Success", attempt)
            return True
        else:
            print("❌")
            if attempt < MAX_RETRIES:
                print(f"   Retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
                save_state(uploaded_files, aip_file, attempt + 1)
                return upload_aip(aip_file, attempt + 1, uploaded_files)
            else:
                log_to_csv(filename, size_mb, "Failed", attempt, result.stderr.strip())
                return False

    except subprocess.TimeoutExpired:
        print("⏰")
        if attempt < MAX_RETRIES:
            print(f"   Retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
            save_state(uploaded_files, aip_file, attempt + 1)
            return upload_aip(aip_file, attempt + 1, uploaded_files)
        else:
            log_to_csv(filename, size_mb, "Failed", attempt, "Timeout")
            return False
    except KeyboardInterrupt:
        print("⏸️  (interrupted)")
        save_state(uploaded_files, aip_file, attempt)
        raise  # Re-raise to exit gracefully

def main():
    """Main execution function with resume support"""
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

    # Check for existing state
    state = load_state()
    uploaded_files = set(state['uploaded_files']) if state else set()
    
    if state:
        print(f"🔄 Resuming from previous session ({len(uploaded_files)} files already uploaded)")
        if state['current_file']:
            print(f"📎 Was processing: {Path(state['current_file']).name}")
    else:
        print("🚀 Starting new upload session")

    # Show files
    print("\nFiles to upload:")
    remaining_files = [f for f in aip_files if f.name not in uploaded_files]
    for f in remaining_files[:3]:
        print(f"   {f.name} ({get_file_size(f)})")
    if len(remaining_files) > 3:
        print(f"   ... and {len(remaining_files) - 3} more")

    # Setup
    print("\n🔐 Checking credentials...")
    check_credentials()
    
    print("🌐 Testing connection...")
    if not test_connection():
        print("❌ Connection failed")
        sys.exit(1)
    
    print("📊 Ensuring containers...")
    ensure_container_exists()
    
    init_csv()

    # Upload files
    print(f"\n🚀 Uploading {len(remaining_files)} files...")
    print("   Press Ctrl+C to pause and resume later\n")
    
    success_count = len(uploaded_files)
    
    try:
        for aip_file in remaining_files:
            if upload_aip(aip_file, uploaded_files=uploaded_files):
                success_count += 1
                
        # Clean up state file on successful completion
        if STATE_FILE.exists():
            STATE_FILE.unlink()
            print("🧹 Cleaned up state file")
            
    except KeyboardInterrupt:
        print(f"\n⏸️  Upload paused. {success_count}/{len(aip_files)} files completed.")
        print(f"💡 Run this script again to resume from where you left off.")
        sys.exit(0)

    # Summary
    print(f"\n📊 Upload complete!")
    print(f"✅ Successful: {success_count}/{len(aip_files)}")
    print(f"❌ Failed: {len(aip_files) - success_count}/{len(aip_files)}")
    
    if success_count == len(aip_files):
        print("🎉 All files uploaded successfully!")
    logger.info(f"Upload summary: {success_count} succeeded, {len(aip_files) - success_count} failed")

if __name__ == "__main__":
    main()