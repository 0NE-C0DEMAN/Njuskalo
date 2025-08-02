import os
import sys
import json
import time
import traceback
import re
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
from functools import partial

# Paths
INPUT_DIR = "backend/website"
OUTPUT_DIR = "backend/json"
LOG_DIR = "backend/logs"
DB_PATH = "backend/phoneDB/phones.db"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_NETWORK_ERROR = 2
EXIT_PARSING_ERROR = 3
EXIT_FS_ERROR = 4

# Configuration
BATCH_SIZE = 50  # Number of files to process in each batch
MAX_WORKERS = min(cpu_count(), 8)  # Use CPU cores but cap at 8 for memory management

# Utils
def log_error(log_path, message):
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now().isoformat()} ERROR {message}\n")

def log_info(log_path, message):
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now().isoformat()} INFO {message}\n")

def generate_log_filename(base_filename):
    base_id = base_filename.split("_")[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_id}_{timestamp}.log"

def process_single_file(filename):
    """Process a single HTML file - this function will be called by each worker process"""
    if not filename.endswith(".html"):
        return None
    
    file_start = time.time()
    filepath = os.path.join(INPUT_DIR, filename)
    base_filename = os.path.splitext(filename)[0]
    
    # Check if JSON already exists - skip if already parsed
    json_file = os.path.join(OUTPUT_DIR, base_filename + ".json")
    if os.path.exists(json_file):
        oglas_id = base_filename.split("_")[0]
        return {
            'filename': filename,
            'status': 'skipped',
            'duration_ms': 0,
            'ad_id': oglas_id
        }
    
    log_filename = generate_log_filename(base_filename)
    log_path = os.path.join(LOG_DIR, log_filename)
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")

        oglas_id = base_filename.split("_")[0]
        podaci = {"id": oglas_id}

        # Canonical link
        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get("href"):
            podaci["link"] = canonical_tag["href"]

        # Location from script tags
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string:
                match = re.search(r'"lat":([\d\.-]+),"lng":([\d\.-]+),"approximate":(true|false)', script.string)
                if match:
                    lat = float(match.group(1))
                    lng = float(match.group(2))
                    approximate = match.group(3) == 'true'
                    podaci["lokacija"] = {
                        "lat": lat,
                        "lng": lng,
                        "approximate": approximate
                    }
                    break

        # Title
        title_tag = soup.find("title")
        naslov = title_tag.get_text(strip=True) if title_tag else None

        # Price
        price_tag = soup.select_one("dl.ClassifiedDetailSummary-priceRow dd.ClassifiedDetailSummary-priceDomestic")
        cijena = price_tag.get_text(strip=True) if price_tag else None

        podaci["naslov"] = naslov
        podaci["cijena"] = cijena

        # Basic details
        info_section = soup.select_one("div.ClassifiedDetailBasicDetails dl.ClassifiedDetailBasicDetails-list")
        if info_section:
            dt_tags = info_section.find_all("dt")
            dd_tags = info_section.find_all("dd")
            for dt, dd in zip(dt_tags, dd_tags):
                key_span = dt.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
                val_span = dd.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
                kljuc = key_span.get_text(strip=True) if key_span else None
                vrijednost = val_span.get_text(strip=True) if val_span else None
                if kljuc and vrijednost:
                    podaci[kljuc] = vrijednost

        # Description
        desc_tag = soup.find("div", class_="ClassifiedDetailDescription-text")
        opis = desc_tag.get_text(" ", strip=True).replace("\n", " ") if desc_tag else None
        podaci["opis"] = opis

        # Additional property groups
        dodatne_sekcije = soup.select("section.ClassifiedDetailPropertyGroups-group")
        for sekcija in dodatne_sekcije:
            naslov_grupe = sekcija.find("h3", class_="ClassifiedDetailPropertyGroups-groupTitle")
            if not naslov_grupe:
                continue
            ime_grupe = naslov_grupe.get_text(strip=True)

            stavke = []
            elementi = sekcija.select("li.ClassifiedDetailPropertyGroups-groupListItem")
            for li in elementi:
                tekst = li.get_text(strip=True)
                if tekst:
                    stavke.append(tekst)

            if stavke:
                podaci[ime_grupe] = stavke

        # Owner details
        owner_section = soup.select_one("div.ClassifiedDetailOwnerDetails")
        if owner_section:
            agencija_tag = owner_section.select_one("h2.ClassifiedDetailOwnerDetails-title a")
            if agencija_tag:
                podaci["naziv_agencije"] = agencija_tag.get_text(strip=True)

            web_tag = owner_section.select_one("a[href^='http']:not([href^='mailto'])")
            if web_tag:
                podaci["profil_agencije"] = web_tag.get("href")

            email_tag = owner_section.select_one("a[href^='mailto']")
            if email_tag:
                podaci["email_agencije"] = email_tag.get_text(strip=True)

            adresa_li = owner_section.select_one("li.ClassifiedDetailOwnerDetails-contactEntry i[aria-label='Adresa']")
            if adresa_li and adresa_li.parent:
                podaci["adresa_agencije"] = adresa_li.parent.get_text(strip=True).replace("Adresa: ", "")

        # Fetch phone from SQLite database
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT phones FROM phones WHERE ad_id = ?", (oglas_id,))
            row = cursor.fetchone()
            if row:
                phones = json.loads(row[0])  # stored as a JSON string array
                phone_clean = phones[0].strip() if phones and isinstance(phones[0], str) else None
                podaci["telefon"] = phone_clean
            else:
                podaci["telefon"] = None
            conn.close()
        except Exception as e:
            podaci["telefon"] = None
            log_error(log_path, f"DB ERROR retrieving phone for ad_id {oglas_id}: {str(e)}")

        # System details
        system_details = soup.select_one("dl.ClassifiedDetailSystemDetails-list")
        if system_details:
            dt_tags = system_details.find_all("dt")
            dd_tags = system_details.find_all("dd")
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                val = dd.get_text(strip=True)
                if key and val:
                    if key == "Oglas objavljen":
                        podaci["oglas_objavljen"] = val
                    elif key == "Do isteka još":
                        podaci["do_isteka"] = val
                    elif key == "Oglas prikazan":
                        podaci["oglas_prikazan"] = val

        # Images
        image_tags = soup.select("li[data-media-type='image']")
        slike = [tag.get("data-large-image-url") for tag in image_tags if tag.get("data-large-image-url")]
        podaci["slike"] = slike

        # Save JSON
        json_ime = base_filename + ".json"
        json_putanja = os.path.join(OUTPUT_DIR, json_ime)
        with open(json_putanja, "w", encoding="utf-8") as jf:
            json.dump(podaci, jf, ensure_ascii=False, indent=2)

        trajanje = int((time.time() - file_start) * 1000)
        log_info(log_path, f"PARSE {filename} SUCCESS {trajanje}ms")
        
        return {
            'filename': filename,
            'status': 'success',
            'duration_ms': trajanje,
            'ad_id': oglas_id
        }

    except Exception as e:
        trajanje = int((time.time() - file_start) * 1000)
        snippet = html[:1000].replace("\n", " ") if 'html' in locals() else "[no HTML loaded]"
        log_error(log_path, f"PARSE {filename} FAILED {trajanje}ms\n{traceback.format_exc()}\nHTML SNIPPET:\n{snippet}")
        
        return {
            'filename': filename,
            'status': 'error',
            'duration_ms': trajanje,
            'error': str(e),
            'ad_id': oglas_id if 'oglas_id' in locals() else 'unknown'
        }

def process_batch(filenames):
    """Process a batch of files using multiprocessing"""
    print(f"[BATCH] Processing {len(filenames)} files with {MAX_WORKERS} workers...")
    
    batch_start = time.time()
    results = []
    
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_single_file, filenames)
    
    batch_duration = time.time() - batch_start
    
    # Filter out None results (non-HTML files)
    results = [r for r in results if r is not None]
    
    success_count = len([r for r in results if r['status'] == 'success'])
    error_count = len([r for r in results if r['status'] == 'error'])
    skipped_count = len([r for r in results if r['status'] == 'skipped'])
    total_files = len(results)
    
    if total_files > 0:
        processed_results = [r for r in results if r['status'] in ['success', 'error']]
        if processed_results:
            avg_duration = sum(r['duration_ms'] for r in processed_results) / len(processed_results)
        else:
            avg_duration = 0
        files_per_second = len(processed_results) / batch_duration if batch_duration > 0 else 0
        
        print(f"[BATCH COMPLETE] {success_count}/{total_files} successful, {error_count} errors, {skipped_count} skipped")
        if processed_results:
            print(f"[BATCH STATS] Avg: {avg_duration:.1f}ms per file, Rate: {files_per_second:.1f} files/sec")
        
        if error_count > 0:
            print(f"[ERRORS] Files with errors:")
            for result in results:
                if result['status'] == 'error':
                    print(f"  - {result['filename']}: {result['error']}")
    
    return results

def main():
    """Main function with batch processing"""
    exit_code = EXIT_SUCCESS
    start_time = time.time()
    
    try:
        # Get all HTML files
        all_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".html")]
        total_files = len(all_files)
        
        if total_files == 0:
            print(f"No HTML files found in {INPUT_DIR}")
            return EXIT_SUCCESS
        
        print(f"[INIT] Found {total_files} HTML files to process")
        print(f"[INIT] Using {MAX_WORKERS} workers, batch size: {BATCH_SIZE}")
        
        # Process in batches
        all_results = []
        processed_count = 0
        
        for i in range(0, total_files, BATCH_SIZE):
            batch_files = all_files[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"\n[BATCH {batch_num}/{total_batches}] Processing files {i+1}-{min(i+BATCH_SIZE, total_files)} of {total_files}")
            
            batch_results = process_batch(batch_files)
            all_results.extend(batch_results)
            processed_count += len(batch_results)
            
            # Progress update
            progress = (processed_count / total_files) * 100
            elapsed = time.time() - start_time
            if processed_count > 0:
                estimated_total = elapsed * (total_files / processed_count)
                remaining = estimated_total - elapsed
                print(f"[PROGRESS] {progress:.1f}% complete, ETA: {remaining:.1f}s")
        
        # Final statistics
        total_elapsed = time.time() - start_time
        success_count = len([r for r in all_results if r['status'] == 'success'])
        error_count = len([r for r in all_results if r['status'] == 'error'])
        skipped_count = len([r for r in all_results if r['status'] == 'skipped'])
        processed_count = success_count + error_count  # Only count actually processed files
        
        print(f"\n[FINAL RESULTS]")
        print(f"Total files found: {len(all_results)}")
        print(f"Successful: {success_count}")
        print(f"Errors: {error_count}")
        print(f"Skipped (already parsed): {skipped_count}")
        print(f"Total time: {total_elapsed:.2f}s")
        if processed_count > 0:
            print(f"Processing rate: {processed_count / total_elapsed:.1f} files/sec")
        
        if error_count > 0:
            exit_code = EXIT_PARSING_ERROR
            print(f"\n[ERROR SUMMARY] {error_count} files failed to process")
        
        # Create summary log
        summary_log_path = os.path.join(LOG_DIR, f"batch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        with open(summary_log_path, "w", encoding="utf-8") as f:
            f.write(f"Batch Processing Summary - {datetime.now().isoformat()}\n")
            f.write(f"Total files: {len(all_results)}\n")
            f.write(f"Successful: {success_count}\n")
            f.write(f"Errors: {error_count}\n")
            f.write(f"Skipped: {skipped_count}\n")
            f.write(f"Total time: {total_elapsed:.2f}s\n")
            if processed_count > 0:
                f.write(f"Processing rate: {processed_count / total_elapsed:.1f} files/sec\n")
            f.write(f"Workers used: {MAX_WORKERS}\n")
            f.write(f"Batch size: {BATCH_SIZE}\n\n")
            
            if error_count > 0:
                f.write("Failed files:\n")
                for result in all_results:
                    if result['status'] == 'error':
                        f.write(f"  {result['filename']}: {result['error']}\n")
            
            if skipped_count > 0:
                f.write("\nSkipped files (already parsed):\n")
                for result in all_results:
                    if result['status'] == 'skipped':
                        f.write(f"  {result['filename']}\n")
        
        print(f"[SUMMARY] Log saved to: {summary_log_path}")
        
    except FileNotFoundError as e:
        exit_code = EXIT_CONFIG_ERROR
        print(f"CONFIG ERROR: {str(e)}")
    except Exception as e:
        exit_code = EXIT_FS_ERROR
        print(f"FATAL ERROR: {str(e)}")
        traceback.print_exc()
    
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
