import hashlib
import os
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

IMAGE_DIR = os.getenv("IMAGE_DIR", str(Path.cwd() / "images"))
LOG_PATH = os.getenv("LOG_PATH", str(Path.cwd() / "broken_images.txt"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "16"))

def verify_single_file(file_path, root, chunk_size=65536):
    """Verifies a single file and returns its relative path if broken/unreadable."""
    expected_hash = file_path.name.split('.')[0].lower()

    # Validate MD5 naming convention
    if len(expected_hash) != 32 or not all(c in '0123456789abcdef' for c in expected_hash):
        return None

    hasher = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)

        actual_hash = hasher.hexdigest()
        if actual_hash != expected_hash:
            return file_path.relative_to(root)

    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return file_path.relative_to(root)  # Treat unreadable files as broken

    return None

def verify_directory():
    root = Path(IMAGE_DIR).resolve()
    log_file = Path(LOG_PATH).resolve()

    print(f"Scanning directory: {root} ...")

    # Collect files first (skipping the log file itself)
    files_to_check = [
        fp for fp in root.rglob("*")
        if fp.is_file() and fp != log_file
    ]

    print(f"Found {len(files_to_check)} candidate files. Verifying using threads...")

    # Process files concurrently using a thread pool
    broken_files = 0
    with open(log_file, "w", encoding="utf-8") as log:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
            futures = {executor.submit(verify_single_file, fp, root): fp for fp in files_to_check}

            for future in tqdm(as_completed(futures), desc="Processing"):
                path = future.result()
                if path is not None:
                    log.write(f"{path}\n")
                    broken_files += 1

    print(f"Total broken or mismatched files found: {broken_files}")

if __name__ == "__main__":
    verify_directory()
