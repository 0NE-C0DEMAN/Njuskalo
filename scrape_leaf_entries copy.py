def phone_already_in_db(ad_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM phones WHERE ad_id=? LIMIT 1", (ad_id,))
    return cursor.fetchone() is not None

# --- Imports and config ---
import os
import json
import asyncio
import random

import sys
import time
from datetime import datetime
import sqlite3
import json

today_str = datetime.now().strftime("%Y-%m-%d")

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# Import Playwright token/cookie fetcher
import importlib.util
spec = importlib.util.spec_from_file_location("bearer_token_finder", os.path.join(os.path.dirname(__file__), "bearer_token_finder.py"))
bearer_token_finder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bearer_token_finder)


# Function to refresh headers and cookies using Playwright
async def refresh_headers_and_cookies():
    print("[INFO] Refreshing headers and cookies using Playwright...")
    token, cookies = await bearer_token_finder.get_bearer_token_and_cookies(headless=True)
    if token:
        HEADERS['authorization'] = f"Bearer {token}"
    if cookies:
        COOKIES.clear()
        COOKIES.update(cookies)
    print("[INFO] Headers and cookies refreshed.")

CHECKPOINTS_DIR = os.path.join(os.path.dirname(__file__), "checkpoints")
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)

# --- Configuration (copied from realstate.py) ---
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7,en-GB;q=0.6,en-US;q=0.5',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.158"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.158", "Google Chrome";v="138.0.7204.158"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
}

COOKIES = {
    # ...existing code...
    '_clsk': '169b6bk%7C1753173777438%7C11%7C1%7Ce.clarity.ms%2Fcollect'
}

ENTRY_LIST_UL_CLASS = "EntityList-items"
ENTRY_ITEM_LI_CLASS = "EntityList-item"
ENTRY_LINK_A_CLASS = "link"

BACKEND_WEBSITE_DIR = os.path.join(os.path.dirname(__file__), "backend", "website")
BACKEND_LOGS_DIR = os.path.join(os.path.dirname(__file__), "backend", "logs")
LEAF_URLS_DIR = os.path.join(os.path.dirname(__file__), "backend", "categories", "leaf_urls")
CATEGORIES_LOGS_DIR = os.path.join(os.path.dirname(__file__), "backend", "categories", "logs")
CATEGORIES_HTMLS_DIR = os.path.join(os.path.dirname(__file__), "backend", "categories", "htmls")
CATEGORIES_TREE_DIR = os.path.join(os.path.dirname(__file__), "backend", "categories", "tree_jsons")
os.makedirs(BACKEND_WEBSITE_DIR, exist_ok=True)
os.makedirs(BACKEND_LOGS_DIR, exist_ok=True)
os.makedirs(LEAF_URLS_DIR, exist_ok=True)
os.makedirs(CATEGORIES_LOGS_DIR, exist_ok=True)
os.makedirs(CATEGORIES_HTMLS_DIR, exist_ok=True)
os.makedirs(CATEGORIES_TREE_DIR, exist_ok=True)
CONCURRENT_LEAFS = 1
CONCURRENT_ENTRIES = 8

 # ...existing code...

# --- Dynamic rotating proxy config ---
PROXY_CONFIG = {
    "http": "http://u07482d15574405cb-zone-custom-region-eu:u07482d15574405cb@118.193.58.115:2334",
    "https": "http://u07482d15574405cb-zone-custom-region-eu:u07482d15574405cb@118.193.58.115:2334"
}

# --- Proxy fallback logic ---
PROXY_LIST = [
    None,  # Local system (no proxy)
    {
        "http": "http://u07482d15574405cb-zone-custom-region-eu:u07482d15574405cb@118.193.58.115:2334",
        "https": "http://u07482d15574405cb-zone-custom-region-eu:u07482d15574405cb@118.193.58.115:2334"
    }
]
use_local_only = False

import logging

def is_proxy_forbidden(response_text):
    if not response_text:
        return False
    forbidden_signals = ["forbidden", "insufficient flow", "errorMsg"]
    return any(sig in response_text.lower() for sig in forbidden_signals)

def extract_entry_urls(html):
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    # Only extract from sections with the correct group title
    valid_titles = {"Njuškalo oglasi", "Sniff ads"}
    for section in soup.find_all("section", class_=lambda c: c and "EntityList" in c):
        h2 = section.find("h2", class_=lambda c: c and "EntityList-groupTitle" in c)
        if not h2:
            continue
        # Extract text, ignoring <font> wrappers
        title_text = h2.get_text(strip=True)
        if title_text not in valid_titles:
            continue
        ul = section.find("ul", class_=lambda c: c and "EntityList-items" in c)
        if not ul:
            continue
        for li in ul.find_all("li", class_=lambda c: c and "EntityList-item" in c):
            a = li.find("a", class_=ENTRY_LINK_A_CLASS)
            if a and a.get("href"):
                href = a["href"]
                if href.startswith("/"):
                    href = "https://www.njuskalo.hr" + href
                urls.append(href)
    return list(set(urls))

async def fetch_html(session, url):
    global use_local_only
    timeout = 15  # seconds
    try:
        if use_local_only:
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                timeout=timeout
            )
        else:
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=PROXY_CONFIG),
                timeout=timeout
            )
        response.raise_for_status()
        text = getattr(response, "text", "")
        if is_proxy_forbidden(text):
            logging.warning("[Proxy] Forbidden or quota exceeded, switching permanently to local system...")
            use_local_only = True
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                timeout=timeout
            )
            response.raise_for_status()
            text = getattr(response, "text", "")
        # ShieldSquare block detection
        import re
        if re.search(r'<title>\s*ShieldSquare Captcha\s*</title>', text, re.IGNORECASE):
            print(f"[BLOCK DETECTED] {url} - Exiting script and pausing for 1 minute...")
            import sys
            import time
            time.sleep(60)
            sys.exit(99)
        return text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        if not use_local_only:
            logging.warning("[Proxy] Exception, switching permanently to local system...")
            use_local_only = True
            try:
                response = await asyncio.wait_for(
                    session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                    timeout=timeout
                )
                response.raise_for_status()
                text = getattr(response, "text", "")
                # ShieldSquare block detection
                import re
                if re.search(r'<title>\s*ShieldSquare Captcha\s*</title>', text, re.IGNORECASE):
                    print(f"[BLOCK DETECTED] {url} - Exiting script and pausing for 1 minute...")
                    import sys
                    import time
                    time.sleep(60)
                    sys.exit(99)
                return text
            except Exception as e2:
                logging.error(f"Local system also failed: {e2}")
        return None


import re

def extract_bearer_token_from_html(html):
    # Try to find Bearer token in JS variables or meta tags
    # Common pattern: '"accessToken":"<token>"' or 'Bearer <token>'
    m = re.search(r'"accessToken"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1)
    m = re.search(r'Bearer ([A-Za-z0-9\-\._~\+\/]+=*)', html)
    if m:
        return m.group(1)
    return None

def extract_ad_id_from_url(entry_url):
    # Extract the number after the last dash at the end of the URL
    m = re.search(r'-([0-9]+)$', entry_url)
    if m:
        return m.group(1)
    return None


async def save_entry_html(session, entry_url):
    ad_id = extract_ad_id_from_url(entry_url)
    if not ad_id:
        print(f"[SKIP] Could not extract ad_id from {entry_url}")
        return False
    db_dir = os.path.join(os.path.dirname(__file__), "backend", "phoneDB")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "phones.db")
    # Create the DB file and table if not present
    if not os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS phones (
                    ad_id TEXT PRIMARY KEY,
                    phones TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()
    conn = sqlite3.connect(db_path)
    if phone_already_in_db(ad_id, conn):
        print(f"[SKIP] Phone already in DB for ad {ad_id}")
        conn.close()
        return False
    conn.close()
    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y%m%d_%H%M%S")
    filename = f"{ad_id}_{now_str}.html"
    save_path = os.path.join(BACKEND_WEBSITE_DIR, filename)
    log_path = os.path.join(BACKEND_LOGS_DIR, filename.replace('.html', '.log'))
    t0 = time.time()
    html = await fetch_html(session, entry_url)
    duration_ms = int((time.time() - t0) * 1000)
    timestamp = datetime.now().isoformat()
    if html:
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
        log_line = f"{timestamp} HTML EXTRACTION {filename} SUCCESS {duration_ms}ms\n"
        print(f"[SAVED] {save_path}")
        with open(log_path, "w", encoding="utf-8") as logf:
            logf.write(log_line)
        return True
    else:
        log_line = f"{timestamp} HTML EXTRACTION {filename} FAILED {duration_ms}ms\n"
        with open(log_path, "w", encoding="utf-8") as logf:
            logf.write(log_line)
        return False


# Per-leaf-URL, per-page checkpointing

# Unified checkpoint file for all leaf URLs and their page progress
def get_unified_checkpoint_file(leaf_file):
    today_str = datetime.now().strftime("%Y-%m-%d")
    base = os.path.basename(leaf_file)
    return os.path.join(CHECKPOINTS_DIR, f"scrape_checkpoint_{today_str}_{base}.json")

def save_unified_checkpoint(leaf_file, leaf_url, page):
    path = get_unified_checkpoint_file(leaf_file)
    data = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                data = {}
    # Store detailed info for each leaf_url
    if leaf_url not in data:
        data[leaf_url] = {}
    data[leaf_url]["last_page"] = page
    data[leaf_url]["last_url_fetched"] = leaf_url if page == 1 else f"{leaf_url}?page={page}"
    data[leaf_url]["timestamp"] = datetime.now().isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)

def load_unified_checkpoint(leaf_file, leaf_url):
    path = get_unified_checkpoint_file(leaf_file)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if leaf_url in data:
                    return data[leaf_url].get("last_page", 1)
                else:
                    return 1
            except Exception:
                return 1
    return 1


async def process_leaf_url(session, leaf_url, leaf_file, progress_callback=None):
    entry_urls = []
    page = load_unified_checkpoint(leaf_file, leaf_url)
    last_page = page
    first_page_urls = None
    prev_page_urls = None
    import re
    while True:
        url = leaf_url if page == 1 else f"{leaf_url}?page={page}"
        html = await fetch_html(session, url)
        save_unified_checkpoint(leaf_file, leaf_url, page)
        if not html:
            print(f"[WARN] Failed to fetch page {page} for {leaf_url}. Skipping this page but will try next page.")
            page += 1
            continue
        canonical_url = None
        base_url = leaf_url.rstrip("/")
        # Only check canonical URL with regex if page > 1
        if page > 1:
            m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE)
            if m:
                canonical_url = m.group(1).rstrip("/")
            if canonical_url and canonical_url == base_url:
                print(f"[INFO] Page {page} for {leaf_url} redirected to page 1 (canonical URL match). Stopping paging for this leaf.")
                break
        page_entry_urls = extract_entry_urls(html)
        if not page_entry_urls:
            break
        # Store first page entry URLs for comparison
        if page == 1:
            first_page_urls = set(page_entry_urls)
        # If current page's entry URLs match previous page's, we've looped or stuck
        if prev_page_urls is not None and set(page_entry_urls) == prev_page_urls:
            print(f"[INFO] Page {page} for {leaf_url} is a repeat of previous page. Stopping paging for this leaf.")
            break
        # If current page's entry URLs match page 1, we've looped back
        if page > 1 and first_page_urls is not None and set(page_entry_urls) == first_page_urls:
            print(f"[INFO] Page {page} for {leaf_url} is a repeat of page 1. Stopping paging for this leaf.")
            break
        entry_urls.extend(page_entry_urls)
        last_page = page
        prev_page_urls = set(page_entry_urls)
        if len(page_entry_urls) < 25:
            break
        page += 1
        await asyncio.sleep(random.uniform(0.5, 1.0))
    entry_urls = list(set(entry_urls))
    saved = 0
    total = len(entry_urls)
    t0 = time.time()
    req_count = 0
    sem = asyncio.Semaphore(CONCURRENT_ENTRIES)
    processed_ads = set()
    async def save_one(entry_url):
        async with sem:
            ad_id = extract_ad_id_from_url(entry_url)
            if ad_id in processed_ads:
                print(f"[SKIP] Already processed ad {ad_id} in this run")
                return False
            ok = await save_entry_html(session, entry_url)
            if ok:
                processed_ads.add(ad_id)
            return ok
    tasks = [save_one(entry_url) for entry_url in entry_urls]
    for i, task in enumerate(asyncio.as_completed(tasks), 1):
        ok = await task
        req_count += 1
        if ok:
            saved += 1
        elapsed = time.time() - t0
        rps = saved / elapsed if elapsed > 0 else 0
        rpm = saved / (elapsed / 60) if elapsed > 0 else 0
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        progress = (f"{now_str} | {leaf_url[:60]:<60} | Page: {last_page} | "
                    f"Success: {saved}/{total} | RPS: {rps:.2f} | RPM: {rpm:.2f}")
        if progress_callback:
            progress_callback(progress)
        else:
            print(progress, end='\r', flush=True)
    print()  # Newline after progress
    return saved



# Per-leaf-url-file checkpointing
def get_checkpoint_file(leaf_file):
    base = os.path.basename(leaf_file)
    today_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(CHECKPOINTS_DIR, f"scrape_checkpoint_{today_str}_{base}.json")

def save_checkpoint(idx, leaf_file):
    with open(get_checkpoint_file(leaf_file), "w", encoding="utf-8") as f:
        json.dump({"last_index": idx}, f)

def load_checkpoint(leaf_file):
    path = get_checkpoint_file(leaf_file)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("last_index", 0)
    return 0


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true", help="Restart from zero, ignore checkpoint.")
    args = parser.parse_args()

    # Use global today_str (do not reassign locally)
    # Find all .txt files in LEAF_URLS_DIR
    leaf_files = [os.path.join(LEAF_URLS_DIR, f) for f in os.listdir(LEAF_URLS_DIR) if f.endswith(f'_{today_str}.txt')]
    if not leaf_files:
        print(f"No .txt files found in {LEAF_URLS_DIR}")
        return

    # Check checkpoint files for today's date in filename
    checkpoint_files_today = [os.path.join(CHECKPOINTS_DIR, f) for f in os.listdir(CHECKPOINTS_DIR)
                             if f.startswith(f"scrape_checkpoint_{today_str}_") and f.endswith(".json")]
    checkpoint_files_old = [os.path.join(CHECKPOINTS_DIR, f) for f in os.listdir(CHECKPOINTS_DIR)
                           if f.startswith("scrape_checkpoint_") and not f.startswith(f"scrape_checkpoint_{today_str}_") and f.endswith(".json")]
    if not checkpoint_files_today and checkpoint_files_old:
        print("No checkpoint files from today. Deleting all old checkpoints and starting fresh.")
        for cp in checkpoint_files_old:
            try:
                os.remove(cp)
            except Exception as e:
                print(f"Could not delete {cp}: {e}")

    for leaf_file in leaf_files:
        print(f"\nProcessing leaf URL file: {leaf_file}")
        with open(leaf_file, "r", encoding="utf-8") as f:
            leaf_urls = [line.strip() for line in f if line.strip()]
        if not leaf_urls:
            print(f"  [SKIP] No URLs in {leaf_file}")
            continue
        start_idx = 0 if args.restart else load_checkpoint(leaf_file)
        total_leaves = len(leaf_urls)
        print(f"  Starting from leaf {start_idx+1} of {total_leaves}")

        sem = asyncio.Semaphore(CONCURRENT_LEAFS)
        async def process_one_leaf(idx, leaf_url):
            async with sem:
                # Refresh headers and cookies after every 50 leaf URLs
                if (idx + 1) % 50 == 0:
                    print(f"[INFO] Refreshing headers and cookies after processing {idx + 1} leaf URLs...")
                    await refresh_headers_and_cookies()
                
                async with AsyncSession() as session:
                    n = await process_leaf_url(session, leaf_url, leaf_file)
                    print(f"Saved {n} entries for {leaf_url}")
                    save_checkpoint(idx+1, leaf_file)
        await asyncio.gather(*(process_one_leaf(idx, url) for idx, url in enumerate(leaf_urls[start_idx:], start=start_idx)))
        print(f"  Done with {leaf_file}. All entry HTMLs saved in '{BACKEND_WEBSITE_DIR}' directory.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
        sys.exit(0)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        print(f"[ERROR] Unhandled exception: {e}")
        sys.exit(1)
