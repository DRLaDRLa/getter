import asyncio
import csv
import os
from pathlib import Path
import aiosqlite
import httpx

DB_PATH = os.getenv("DB_PATH", "downloaded_posts.db")
IMAGE_DIR = os.getenv("IMAGE_DIR", str(Path.cwd() / "images"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "8"))

Path(IMAGE_DIR).mkdir(parents=True, exist_ok=True)

async def init_db():
    """Initialize the SQLite database and create the table with a UNIQUE filename constraint."""
    async with aiosqlite.connect(DB_PATH) as db:# Enable Write-Ahead Logging for concurrent read/write handling
        await db.execute("PRAGMA journal_mode=WAL;")
        # Tell SQLite to wait up to 5000ms (5 seconds) if the database is busy
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def log_download(filename: str):
    """Safely log the downloaded file name to SQLite, ignoring duplicates."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Ensure timeout applies to this connection as well
        await db.execute("PRAGMA busy_timeout=5000;")
        try:
            # INSERT OR IGNORE ensures that duplicate filenames won't throw an error
            # and will simply be skipped if they already exist in the database.
            await db.execute(
                "INSERT OR IGNORE INTO downloaded_files (filename) VALUES (?)",
                (filename,)
            )
            await db.commit()
        except Exception as e:
            print(f"[DB ERROR] Failed to log {filename}: {e}")

async def download_file(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, url: str, filename: str):
    """Downloads a single file and logs it to the database upon success."""
    destination_path = Path(IMAGE_DIR) / filename

    async with semaphore:
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)

            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()

            # Offload synchronous disk I/O to a background thread
            await asyncio.to_thread(destination_path.write_bytes, response.content)
            print(f"[SUCCESS] Downloaded: {destination_path}")

            # # Log the filename to SQLite
            # await log_download(filename)

        except httpx.HTTPStatusError as e:
            print(f"[HTTP ERROR] Failed to download {filename}: {e.response.status_code}")
        except Exception as e:
            print(f"[ERROR] Failed to download {filename}: {e}")

async def filter_unprocessed_files(files_to_download: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Queries the database, logs how many items are missing, and returns them."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT filename FROM downloaded_files") as cursor:
            rows = await cursor.fetchall()
            downloaded_set = {row[0] for row in rows}

    # Files missing from the database
    missing_files = [
        (url, filename) for url, filename in files_to_download
        if filename not in downloaded_set
    ]

    already_logged_count = len(files_to_download) - len(missing_files)
    missing_count = len(missing_files)

    print(f"[INFO] Status Check: {already_logged_count} already in DB, {missing_count} missing from DB.")

    return missing_files

async def sync_local_files_to_db():
    """
    Scans the local storage directory, logs how many files were found on disk,
    backfills any missing from the database, and returns the total count found.
    """
    image_dir_path = Path(IMAGE_DIR)
    if not image_dir_path.exists():
        print(f"[SYNC] Directory '{IMAGE_DIR}' does not exist yet. 0 files found on disk.")
        return 0

    # Find all files recursively on disk
    local_files = [
        (p.relative_to(image_dir_path).as_posix(),)
        for p in image_dir_path.rglob("*") if p.is_file()
    ]

    total_found_on_disk = len(local_files)
    print(f"[SYNC] Found {total_found_on_disk} total file(s) on disk in '{IMAGE_DIR}'.")

    if total_found_on_disk == 0:
        return 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=5000;")

        # Fetch current database records
        async with db.execute("SELECT filename FROM downloaded_files") as cursor:
            rows = await cursor.fetchall()
            existing_set = {row[0] for row in rows}

        # Isolate disk files that are missing from the DB
        missing_files = [f for f in local_files if f[0] not in existing_set]
        missing_count = len(missing_files)

        if missing_count > 0:
            await db.executemany(
                "INSERT OR IGNORE INTO downloaded_files (filename) VALUES (?)",
                missing_files
            )
            await db.commit()
            print(f"[SYNC] Backfilled {missing_count} unrecorded file(s) into the database.")
        else:
            print("[SYNC] All files found on disk are already logged in the database.")

async def main(files_to_download):
    # 1. Initialize the database table before running concurrent downloads
    await init_db()

    # 2. Sync any unlogged files currently sitting on disk into the database
    await sync_local_files_to_db()

    # 2. Filter out files that have already been downloaded
    active_downloads = await filter_unprocessed_files(files_to_download)

    if not active_downloads:
        print("[INFO] All files are already downloaded. Nothing to do!")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [
            download_file(client, semaphore, url, filename)
            for url, filename in files_to_download
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    with open('posts.csv', newline='') as csvfile:
        files_to_download = []
        posts = csv.DictReader(csvfile)
        deleted = 0
        for row in posts:
            is_deleted = row['is_deleted']
            if is_deleted == 't':
                deleted += 1
                continue
            md5 = row['md5']
            file_ext = row['file_ext']
            path = f'{md5[0:2]}/{md5[2:4]}/{md5}.{file_ext}'
            files_to_download.append((f'https://static1.e6ai.net/data/{path}', f'{path}'))
    print(f"[INFO] Found {len(files_to_download)} post to download ({deleted} deleted skipped).")
    asyncio.run(main(files_to_download))
