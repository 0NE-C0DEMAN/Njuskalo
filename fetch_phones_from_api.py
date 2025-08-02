
import os
import re
import json
import asyncio
import sqlite3
from datetime import datetime
from curl_cffi.requests import AsyncSession
import logging


# ---- CONFIGURATION ----
# No hardcoded BEARER_TOKEN or COOKIES; always use fresh from Playwright script


# Directory containing all entry HTMLs (use backend/website)
target_dir = os.path.join(os.path.dirname(__file__), "backend", "website")




 # SQLite DB setup in backend/phoneDB folder
phone_db_dir = os.path.join(os.path.dirname(__file__), "backend", "phoneDB")
os.makedirs(phone_db_dir, exist_ok=True)
db_path = os.path.join(phone_db_dir, "phones.db")

# --- LOGGING SETUP ---
log_path = os.path.join(phone_db_dir, "phones.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
def init_db():
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS phones (
            ad_id TEXT PRIMARY KEY,
            phones TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_phones_to_db(ad_id, phone_list):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Always overwrite (upsert)
    c.execute("REPLACE INTO phones (ad_id, phones) VALUES (?, ?)",
              (ad_id, json.dumps(phone_list, ensure_ascii=False) if phone_list is not None else None))
    conn.commit()
    conn.close()



# Extract ad id as the number at the start of the filename before the first underscore
ad_id_re = re.compile(r"^([0-9]+)_")

# Njuskalo phone API endpoint
def phone_api_url(ad_id):
    return f"https://www.njuskalo.hr/ccapi/v4/phone-numbers/ad/{ad_id}"

import random

import importlib.util
import sys
import asyncio

spec = importlib.util.spec_from_file_location("bearer_token_finder", os.path.join(os.path.dirname(__file__), "bearer_token_finder.py"))
if spec is None or spec.loader is None:
    raise ImportError("Could not load bearer_token_finder.py module spec or loader.")
bearer_token_finder = importlib.util.module_from_spec(spec)
sys.modules["bearer_token_finder"] = bearer_token_finder
spec.loader.exec_module(bearer_token_finder)

PROXY_LIST = [
    None  # Local system (no proxy)
]


# --- Token/cookie refresh logic ---
async def get_token_and_cookies():
    # Call the Playwright async function directly
    return await bearer_token_finder.get_bearer_token_and_cookies()

async def fetch_phone_number(session, ad_id, bearer_token, cookies):
    url = phone_api_url(ad_id)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": f"https://www.njuskalo.hr/nekretnine/*-oglas-{ad_id}",
    }
    proxy_cfg = None  # Always use local, no proxy
    try:
        resp = await session.get(url, headers=headers, cookies=cookies, timeout=15, proxies=proxy_cfg)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"ad_id {ad_id}: {e}")
        if '401' in str(e):
            return 'REFRESH_TOKEN'
        return None

def find_all_html_files():
    html_files = []
    for root, dirs, files in os.walk(target_dir):
        for fname in files:
            if fname.endswith(".html"):
                html_files.append(os.path.join(root, fname))
    return html_files


def extract_ad_id_from_filename(filename):
    # Normalize path separators and just use the filename
    fname = os.path.basename(filename)
    m = ad_id_re.match(fname)
    return m.group(1) if m else None

def extract_time_from_html(html_path):
    # Try to extract the time from the HTML file (from meta or script tags)
    # If not found, use file modified time
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        # Try to find ISO date in the HTML (e.g. 2025-07-25T15:30:00)
        m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', html)
        if m:
            return m.group(1)
    except Exception:
        pass
    # Fallback: file modified time
    ts = os.path.getmtime(html_path)
    return datetime.fromtimestamp(ts).isoformat()

async def process_file(session, html_path, bearer_token, cookies):
    ad_id = extract_ad_id_from_filename(html_path)
    if not ad_id:
        logging.warning(f"[SKIP] Could not extract ad_id from {html_path}")
        return 'OK'

    data = await fetch_phone_number(session, ad_id, bearer_token, cookies)

    if data == 'REFRESH_TOKEN':
        return 'REFRESH_TOKEN'

    numbers = []
    try:
        numbers = [
            n["formattedNumber"]
            for n in data["data"]["attributes"]["numbers"]
            if n.get("formattedNumber")
        ]
    except Exception as e:
        logging.warning(f"[WARN] Failed to parse phone data for ad {ad_id}: {e}")

    if numbers:
        logging.info(f"[OK] Found {len(numbers)} phone(s) for ad {ad_id}")
    else:
        logging.info(f"[INFO] No phone numbers found for ad {ad_id}, saving null")

    # Save to DB (even if empty/null)
    save_phones_to_db(ad_id, numbers if numbers else None)

    return 'OK'


async def main():
    html_files = find_all_html_files()
    logging.info(f"Found {len(html_files)} HTML files.")


    # --- FLAG: re-scrape ad_ids with null phone numbers ---
    RESCRAPE_NULL_PHONES = False  # Set to False to skip nulls, True to re-scrape nulls

    # Load ad_ids and their phone values from DB
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT ad_id, phones FROM phones")
    adid_to_phones = {row[0]: row[1] for row in c.fetchall()}
    conn.close()

    files_to_process = []
    skipped = 0
    rescrape_count = 0
    for path in html_files:
        ad_id = extract_ad_id_from_filename(path)
        if not ad_id:
            continue
        if ad_id in adid_to_phones:
            phones_val = adid_to_phones[ad_id]
            if phones_val is None or phones_val == 'null':
                if RESCRAPE_NULL_PHONES:
                    rescrape_count += 1
                    files_to_process.append(path)
                else:
                    skipped += 1
                continue
            skipped += 1
            continue
        files_to_process.append(path)
    logging.info(f"Skipping {skipped} files already in DB with phones. Re-scraping {rescrape_count} with null phones. {len(files_to_process)} files left to process.")

    # Get initial token and cookies
    bearer_token, cookies = await get_token_and_cookies()
    logging.info("\n[INFO] Using Bearer token:")
    logging.info(bearer_token)
    logging.info("\n[INFO] Using cookies:")
    logging.info(cookies)
    if not bearer_token or not cookies:
        logging.error("Could not get Bearer token or cookies. Exiting.")
        return
    async with AsyncSession() as session:
        i = 0
        BATCH_SIZE = 50
        while i < len(files_to_process):
            batch = files_to_process[i:i+BATCH_SIZE]
            results = await asyncio.gather(*[process_file(session, path, bearer_token, cookies) for path in batch])
            # If any batch result is 'REFRESH_TOKEN', refresh and retry that batch
            if 'REFRESH_TOKEN' in results:
                logging.warning("Refreshing Bearer token and cookies due to 401 error...")
                bearer_token, cookies = await get_token_and_cookies()
                logging.info("\n[INFO] Using Bearer token:")
                logging.info(bearer_token)
                logging.info("\n[INFO] Using cookies:")
                logging.info(cookies)
                if not bearer_token or not cookies:
                    logging.error("Could not refresh Bearer token or cookies. Exiting.")
                    return
                # Retry the same batch
                continue
            i += BATCH_SIZE
            # await asyncio.sleep(random.uniform(0.8, 1.2))


if __name__ == "__main__":
    init_db()
    asyncio.run(main())
