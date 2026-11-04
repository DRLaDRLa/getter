import asyncio
import csv
import os
from pathlib import Path
import aiosqlite
import httpx

IMAGE_DIR = os.getenv("IMAGE_DIR", str(Path.cwd() / "images"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "16"))

Path(IMAGE_DIR).mkdir(parents=True, exist_ok=True)

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

async def filter_unprocessed_files(files_to_download: list[tuple[str, str]], downloaded_files: set[str]) -> list[tuple[str, str]]:
    missing_files = [
        (url, filename) for url, filename in files_to_download
        if filename not in downloaded_files
    ]

    already_logged_count = len(files_to_download) - len(missing_files)
    missing_count = len(missing_files)

    print(f"[INFO] Status Check: {already_logged_count} already downloaded, {missing_count} missing to download.")

    return missing_files

def get_downloaded_files() -> set[str]:
    """
    Scans the local storage directory, logs how many files were found on disk,
    backfills any missing from the database, and returns the total count found.
    """
    image_dir_path = Path(IMAGE_DIR)
    if not image_dir_path.exists():
        print(f"[SYNC] Directory '{IMAGE_DIR}' does not exist yet. 0 files found on disk.")
        return set()

    # Find all files recursively on disk
    local_files = {
        p.relative_to(image_dir_path).as_posix()
        for p in image_dir_path.rglob("*") if p.is_file()
    }

    total_found_on_disk = len(local_files)
    print(f"[SYNC] Found {total_found_on_disk} total file(s) on disk in '{IMAGE_DIR}'.")

    return local_files

async def main(files_to_download):
    downloaded_files = get_downloaded_files()
    active_downloads = await filter_unprocessed_files(files_to_download, downloaded_files)

    if not active_downloads:
        print("[INFO] All files are already downloaded. Nothing to do!")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [
            download_file(client, semaphore, url, filename)
            for url, filename in active_downloads
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
    print(f"[INFO] Found {len(files_to_download)} posts ({deleted} deleted skipped).")
    asyncio.run(main(files_to_download))
