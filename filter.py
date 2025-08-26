import csv
from pathlib import Path
import shutil

#Extract successfully uploaded filenames from CSV into a new folder

UPLOAD_DIR = Path("/Volumes/Vintage-1/archCoppul/Under4gb/")
ARCHIVE_DIR = Path("/Volumes/Vintage-1/archCoppul/Already_Uploaded_5-12_")
CSV_LOG = Path("/Users/Daniel/Desktop/Arch/arch-importer/5-12-upload-summary.csv")
 
#Delete cache if you are moving and running locally `find . -name "__pycache__" -exec rm -rf {} +`


def get_uploaded_from_log():
    """Extract successfully uploaded filenames from CSV"""
    uploaded = set()
    with open(CSV_LOG, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header
        for row in reader:
            if len(row) > 2 and row[2].lower() == "success":  # Status is now column 2
                uploaded.add(row[0])  # Filename is column 0
    return uploaded

def move_uploaded_files():
    """Move files listed as 'Success' in log to archive"""
    ARCHIVE_DIR.mkdir(exist_ok=True, parents=True)
    uploaded_files = get_uploaded_from_log()
    
    moved = 0
    for file in UPLOAD_DIR.glob("*.7z"):
        if file.name in uploaded_files:
            dest = ARCHIVE_DIR / file.name
            shutil.move(str(file), str(dest))
            print(f"✓ Moved {file.name}")
            moved += 1
    
    print(f"\nDone. Moved {moved} files to {ARCHIVE_DIR}")

if __name__ == "__main__":
    print(f"Checking log: {CSV_LOG}")
    print(f"Source dir: {UPLOAD_DIR}")
    print(f"Archive dir: {ARCHIVE_DIR}")
    move_uploaded_files()