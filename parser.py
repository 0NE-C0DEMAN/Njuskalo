import os
import sys
import json
import time
import traceback
import re
from datetime import datetime
from bs4 import BeautifulSoup

# Paths
INPUT_DIR = "backend/website"
OUTPUT_DIR = "backend/json"
LOG_DIR = "backend/logs"
PARSED_LOG_PATH = "backend/parsed.log"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_NETWORK_ERROR = 2
EXIT_PARSING_ERROR = 3
EXIT_FS_ERROR = 4

# Track already parsed files
parsed_files = set()
if os.path.exists(PARSED_LOG_PATH):
    with open(PARSED_LOG_PATH, "r", encoding="utf-8") as f:
        parsed_files = set(line.strip() for line in f if line.strip())

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

# Main
exit_code = EXIT_SUCCESS
start_time = time.time()

try:
    log_info_path = None

    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".html") or filename in parsed_files:
            continue

        file_start = time.time()
        filepath = os.path.join(INPUT_DIR, filename)
        base_filename = os.path.splitext(filename)[0]
        log_filename = generate_log_filename(base_filename)
        log_path = os.path.join(LOG_DIR, log_filename)
        log_info_path = log_path

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                html = f.read()

            soup = BeautifulSoup(html, "html.parser")

            # ID iz naziva datoteke
            oglas_id = base_filename.split("_")[0]
            podaci = {"id": oglas_id}

            # Link iz <link rel="canonical"> taga
            canonical_tag = soup.find("link", rel="canonical")
            if canonical_tag and canonical_tag.get("href"):
                podaci["link"] = canonical_tag["href"]

            # Lokacija (lat, lng) iz JavaScript dijela
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

            # Naslov
            title_tag = soup.find("title")
            naslov = title_tag.get_text(strip=True) if title_tag else None

            # Cijena
            price_tag = soup.select_one("dl.ClassifiedDetailSummary-priceRow dd.ClassifiedDetailSummary-priceDomestic")
            cijena = price_tag.get_text(strip=True) if price_tag else None

            podaci["naslov"] = naslov
            podaci["cijena"] = cijena

            # Osnovni podaci
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

            # Opis
            desc_tag = soup.find("div", class_="ClassifiedDetailDescription-text")
            opis = desc_tag.get_text(" ", strip=True).replace("\n", " ") if desc_tag else None
            podaci["opis"] = opis

            # Dodatne informacije
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

            # Agencija
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
                    podaci["adresa_agencije"] = adresa_li.parent.get_text(strip=True).replace("Adresa: ", "")

                telefon_prisutan = owner_section.select_one(".UserPhoneNumber-callSeller")
                podaci["telefon"] = bool(telefon_prisutan)

            # Sustavski podaci oglasa (objavljen, ističe, prikazan)
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

            # Slike
            image_tags = soup.select("li[data-media-type='image']")
            slike = [tag.get("data-large-image-url") for tag in image_tags if tag.get("data-large-image-url")]
            podaci["slike"] = slike

            # Spremi JSON
            json_ime = base_filename + ".json"
            json_putanja = os.path.join(OUTPUT_DIR, json_ime)
            with open(json_putanja, "w", encoding="utf-8") as jf:
                json.dump(podaci, jf, ensure_ascii=False, indent=2)

            # Zapiši kao parsiran
            with open(PARSED_LOG_PATH, "a", encoding="utf-8") as pf:
                pf.write(filename + "\n")

            trajanje = int((time.time() - file_start) * 1000)
            log_info(log_path, f"PARSE {filename} SUCCESS {trajanje}ms")

        except Exception as e:
            trajanje = int((time.time() - file_start) * 1000)
            snippet = html[:1000].replace("\n", " ") if 'html' in locals() else "[no HTML loaded]"
            log_error(log_path, f"PARSE {filename} FAILED {trajanje}ms\n{traceback.format_exc()}\nHTML SNIPPET:\n{snippet}")
            exit_code = EXIT_PARSING_ERROR
            raise e

except FileNotFoundError as e:
    exit_code = EXIT_CONFIG_ERROR
    if log_info_path:
        log_error(log_info_path, f"CONFIG ERROR: {str(e)}\n{traceback.format_exc()}")
except Exception as e:
    exit_code = EXIT_FS_ERROR
    if log_info_path:
        log_error(log_info_path, f"FATAL ERROR: {str(e)}\n{traceback.format_exc()}")
finally:
    elapsed = int((time.time() - start_time) * 1000)
    if log_info_path:
        log_info(log_info_path, f"FINISHED processing in {elapsed}ms.")
    sys.exit(exit_code)