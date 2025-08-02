import os
import sys
import time
import subprocess
import traceback
from datetime import datetime
try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init()
except ImportError:
    class Dummy:
        RESET = RESET_ALL = RED = GREEN = YELLOW = CYAN = MAGENTA = BLUE = ''
    Fore = Style = Dummy()

# Pipeline steps and their scripts
PIPELINE = [
    ("Category Tree Scraper", "njuskalo_category_tree_scraper.py"),
    ("Extract Leaf URLs", "extract_leaf_urls.py"),
    ("Scrape Leaf Entries", "scrape_leaf_entries.py"),
    ("Fetch Phones", "fetch_phones_from_api.py"),
    ("Parse Entries", "parser.py"),
]


from datetime import datetime
today_str = datetime.now().strftime("%Y-%m-%d")
CHECKPOINT_FILE = f"pipeline_checkpoint_{today_str}.txt"
LOG_FILE = f"pipeline_run_{today_str}.log"


def log(msg, color=None, end="\n"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{now}] "
    if color:
        print(color + prefix + msg + Style.RESET_ALL, end=end)
    else:
        print(prefix + msg, end=end)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(prefix + msg + "\n")

def save_checkpoint(idx):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        f.write(str(idx))

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try:
                return int(f.read().strip())
            except Exception:
                return 0
    return 0

def run_step(idx, name, script):
    log(f"Starting step {idx+1}/{len(PIPELINE)}: {name}", Fore.CYAN)
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        elapsed = int(time.time() - start)
        if result.returncode == 0:
            log(f"[SUCCESS] {name} finished in {elapsed}s", Fore.GREEN)
            log(f"Output:\n{(result.stdout or '').strip()}", Fore.GREEN)
            save_checkpoint(idx+1)
            return True
        else:
            log(f"[FAIL] {name} failed in {elapsed}s", Fore.RED)
            log(f"Stdout:\n{(result.stdout or '').strip()}", Fore.YELLOW)
            log(f"Stderr:\n{(result.stderr or '').strip()}", Fore.RED)
            return False
    except Exception as e:
        log(f"[ERROR] Exception in {name}: {e}\n{traceback.format_exc()}", Fore.RED)
        return False

def main():
    log("\n" + "="*50)
    log("PIPELINE RUN STARTED", Fore.MAGENTA)
    log("="*50 + "\n")
    resume = False
    if os.path.exists(CHECKPOINT_FILE):
        log("Checkpoint file found.", Fore.YELLOW)
        resume = True
    idx = load_checkpoint() if resume else 0
    if idx > 0:
        log(f"Resuming from step {idx+1}: {PIPELINE[idx][0]}", Fore.YELLOW)
    else:
        log("Starting from the beginning.", Fore.CYAN)
    for i in range(idx, len(PIPELINE)):
        name, script = PIPELINE[i]
        log("\n" + "-"*40)
        ok = run_step(i, name, script)
        if not ok:
            log(f"Pipeline stopped at step {i+1}: {name}", Fore.RED)
            log("You can fix the issue and rerun to resume from this step.", Fore.YELLOW)
            return
    log("\n" + "="*50)
    log("PIPELINE COMPLETED SUCCESSFULLY!", Fore.GREEN)
    log("="*50 + "\n")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    main()
