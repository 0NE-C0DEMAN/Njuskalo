#!/usr/bin/env python3
"""
Enhanced Proxy Testing Script with Anti-Detection
Tests proxies with randomized headers, delays, and behavioral mimicking.
"""

import sys
import os
import json
import asyncio
import time
import random
from datetime import datetime
import requests
from curl_cffi.requests import AsyncSession
from pathlib import Path

# --- Safe print for Unicode output ---
def safe_print(s):
    try:
        sys.stdout.buffer.write((str(s) + "\n").encode("utf-8"))
        sys.stdout.flush()
    except Exception:
        print(s)

# --- Webshare API Configuration ---
WEBSHARE_API_TOKEN = "wemj46xw6m0q876m6i4x65j434bsh735dbef70hc"

def fetch_webshare_proxies(api_token, page_size=1000):
    """Fetch proxy list from Webshare API"""
    url = f"https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size={page_size}"
    headers = {"Authorization": f"Token {api_token}"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        proxies = []
        for i, p in enumerate(data.get("results", []), 1):
            if p.get("valid"):
                proxy_url = f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
                proxy_info = {
                    "id": i,
                    "address": f"{p['proxy_address']}:{p['port']}",
                    "username": p['username'],
                    "config": {"http": proxy_url, "https": proxy_url}
                }
                proxies.append(proxy_info)
        safe_print(f"[PROXY API] Fetched {len(proxies)} valid proxies from Webshare")
        return proxies
    except Exception as e:
        safe_print(f"[PROXY API ERROR] Failed to fetch proxies: {e}")
        return []

# --- Anti-Detection Headers Pool ---
def get_random_headers():
    """Generate randomized headers to avoid detection"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
    ]
    
    accept_languages = [
        'en-US,en;q=0.9',
        'en-GB,en;q=0.9',
        'en-US,en;q=0.8,hr;q=0.6',
        'hr-HR,hr;q=0.9,en;q=0.8',
        'en,hr;q=0.9'
    ]
    
    return {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': random.choice(accept_languages),
        'cache-control': random.choice(['no-cache', 'max-age=0']),
        'pragma': 'no-cache',
        'sec-ch-ua': f'"Not_A Brand";v="8", "Chromium";v="{random.randint(120, 131)}", "Google Chrome";v="{random.randint(120, 131)}"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': random.choice(['"Windows"', '"macOS"', '"Linux"']),
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': random.choice(['none', 'same-origin', 'cross-site']),
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': random.choice(user_agents)
    }

# Enhanced cookies with more variety
def get_random_cookies():
    """Generate randomized cookies to appear more human"""
    base_cookies = {
        'USESSIONID': f'{random.randint(1000000, 9999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}',
        'sourceType': random.choice(['DIRECT', 'SEARCH', 'SOCIAL']),
        'sourceUrl': '',
        '_gid': f'GA1.2.{random.randint(1000000000, 9999999999)}.{int(time.time())}',
        '_ga': f'GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}',
        'highDensity': random.choice(['true', 'false']),
        'OptanonConsent': f'isGpcEnabled=0&datestamp={datetime.now().strftime("%a+%b+%d+%Y+%H%%3A%M%%3A%S+GMT%%2B0100+(Central+European+Standard+Time)")}&version=202504.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0004%3A1%2CC0002%3A1%2CC0003%3A1&geolocation=HR%3A21&AwaitingReconsent=false'
    }
    return base_cookies

# Test configuration
TEST_URL = "https://www.njuskalo.hr/prodaja-stanova/bjelovar"
TIMEOUT = 20  # Increased timeout
CONCURRENCY = 5  # Reduced concurrency to appear less aggressive
MIN_DELAY = 1.0  # Minimum delay between requests
MAX_DELAY = 3.0  # Maximum delay between requests

def is_proxy_forbidden(response_text):
    """Check if response indicates proxy is forbidden/blocked"""
    if not response_text:
        return False
    forbidden_signals = ["forbidden", "insufficient flow", "errorMsg", "access denied"]
    return any(sig in response_text.lower() for sig in forbidden_signals)

def is_block_page(html):
    """Check if page is a block/captcha page"""
    if not html:
        return False
    import re
    # Check for various anti-bot systems
    block_patterns = [
        r'<title>\s*ShieldSquare Captcha\s*</title>',
        r'cloudflare.*challenge',
        r'access.*denied',
        r'captcha',
        r'verification.*required',
        r'please.*verify',
        r'bot.*detected'
    ]
    for pattern in block_patterns:
        if re.search(pattern, html, re.IGNORECASE):
            return True
    return False

def get_block_type(html):
    """Identify the type of blocking system"""
    if not html:
        return "UNKNOWN"
    
    html_lower = html.lower()
    if "shieldsquare" in html_lower:
        return "SHIELDSQUARE"
    elif "cloudflare" in html_lower:
        return "CLOUDFLARE"
    elif "access denied" in html_lower:
        return "ACCESS_DENIED"
    elif "captcha" in html_lower:
        return "CAPTCHA"
    else:
        return "OTHER_BLOCK"

async def test_proxy_with_stealth(session, proxy_info, test_url, output_dir, semaphore):
    """Test a single proxy with enhanced stealth techniques"""
    async with semaphore:
        proxy_id = proxy_info["id"]
        proxy_address = proxy_info["address"]
        proxy_config = proxy_info["config"]
        
        # Random delay before request to avoid pattern detection
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        await asyncio.sleep(delay)
        
        start_time = time.time()
        result = {
            "proxy_id": proxy_id,
            "proxy_address": proxy_address,
            "status": "FAILED",
            "response_time_ms": 0,
            "content_length": 0,
            "error": None,
            "is_blocked": False,
            "is_forbidden": False,
            "block_type": None,
            "timestamp": datetime.now().isoformat(),
            "delay_used": round(delay, 2)
        }
        
        try:
            safe_print(f"[{proxy_id:4d}/1000] Testing proxy {proxy_address} (delay: {delay:.1f}s)...")
            
            # Use randomized headers and cookies for each request
            headers = get_random_headers()
            cookies = get_random_cookies()
            
            response = await asyncio.wait_for(
                session.get(test_url, headers=headers, cookies=cookies, 
                          impersonate="chrome110", proxies=proxy_config),
                timeout=TIMEOUT
            )
            
            response.raise_for_status()
            html_content = response.text
            
            # Check for various error conditions
            result["response_time_ms"] = int((time.time() - start_time) * 1000)
            result["content_length"] = len(html_content)
            
            if is_block_page(html_content):
                result["status"] = "BLOCKED"
                result["is_blocked"] = True
                result["block_type"] = get_block_type(html_content)
                result["error"] = f"{result['block_type']} detected"
            elif is_proxy_forbidden(html_content):
                result["status"] = "FORBIDDEN"
                result["is_forbidden"] = True
                result["error"] = "Proxy forbidden/insufficient flow"
            else:
                result["status"] = "SUCCESS"
                # Save successful HTML to file
                html_filename = f"proxy_{proxy_id:04d}_{proxy_address.replace(':', '_')}.html"
                html_filepath = output_dir / "success" / html_filename
                try:
                    with open(html_filepath, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    
                    # Also save a small sample for verification
                    sample_filename = f"sample_{proxy_id:04d}.txt"
                    sample_filepath = output_dir / "samples" / sample_filename
                    sample_text = html_content[:500] + "..." if len(html_content) > 500 else html_content
                    with open(sample_filepath, "w", encoding="utf-8") as f:
                        f.write(f"Proxy: {proxy_address}\n")
                        f.write(f"Response Time: {result['response_time_ms']}ms\n")
                        f.write(f"Content Length: {result['content_length']}\n")
                        f.write(f"Sample Content:\n{sample_text}")
                        
                except Exception as save_error:
                    safe_print(f"[{proxy_id:4d}] Warning: Could not save HTML: {save_error}")
                    
        except asyncio.TimeoutError:
            result["error"] = "Timeout"
            result["response_time_ms"] = TIMEOUT * 1000
        except Exception as e:
            result["error"] = str(e)
            result["response_time_ms"] = int((time.time() - start_time) * 1000)
        
        # Log result with enhanced status
        status_symbol = {
            "SUCCESS": "✓",
            "FAILED": "✗", 
            "BLOCKED": "🚫",
            "FORBIDDEN": "⛔"
        }.get(result["status"], "?")
        
        error_detail = ""
        if result.get("block_type"):
            error_detail = f" [{result['block_type']}]"
        elif result.get("error"):
            error_detail = f" [{result['error'][:30]}...]" if len(result['error']) > 30 else f" [{result['error']}]"
        
        safe_print(f"[{proxy_id:4d}/1000] {status_symbol} {proxy_address} - {result['status']} "
                  f"({result['response_time_ms']}ms){error_detail}")
        
        return result

async def test_all_proxies_stealth():
    """Test all proxies with enhanced stealth techniques"""
    safe_print("=" * 80)
    safe_print("ENHANCED WEBSHARE PROXY TESTING SCRIPT (ANTI-DETECTION)")
    safe_print("=" * 80)
    safe_print(f"Test URL: {TEST_URL}")
    safe_print(f"Timeout: {TIMEOUT}s")
    safe_print(f"Concurrency: {CONCURRENCY}")
    safe_print(f"Request Delay: {MIN_DELAY}-{MAX_DELAY}s")
    safe_print("Anti-Detection: ✓ Random Headers ✓ Random Cookies ✓ Random Delays")
    safe_print("=" * 80)
    
    # Setup directories
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(__file__).parent / "proxy_test_results_stealth" / timestamp
    (output_dir / "success").mkdir(parents=True, exist_ok=True)
    (output_dir / "samples").mkdir(parents=True, exist_ok=True)
    (output_dir / "failed").mkdir(parents=True, exist_ok=True)
    
    # Fetch proxies
    safe_print("\n[1/4] Fetching proxies from Webshare API...")
    proxy_list = fetch_webshare_proxies(WEBSHARE_API_TOKEN)
    
    if not proxy_list:
        safe_print("❌ No proxies available. Exiting.")
        return
    
    safe_print(f"✓ Retrieved {len(proxy_list)} proxies")
    
    # Test proxies
    safe_print(f"\n[2/4] Testing {len(proxy_list)} proxies with anti-detection...")
    safe_print("-" * 80)
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results = []
    
    async with AsyncSession() as session:
        tasks = [
            test_proxy_with_stealth(session, proxy_info, TEST_URL, output_dir, semaphore)
            for proxy_info in proxy_list
        ]
        
        # Execute tests with progress tracking
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            result = await task
            results.append(result)
            
            # Progress update every 25 proxies
            if i % 25 == 0:
                safe_print(f"\n--- Progress: {i}/{len(proxy_list)} proxies tested ---")
    
    # Generate enhanced statistics
    safe_print("\n" + "=" * 80)
    safe_print("[3/4] Generating enhanced test results...")
    
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")
    blocked_count = sum(1 for r in results if r["status"] == "BLOCKED")
    forbidden_count = sum(1 for r in results if r["status"] == "FORBIDDEN")
    
    # Analyze block types
    block_types = {}
    for r in results:
        if r.get("block_type"):
            block_types[r["block_type"]] = block_types.get(r["block_type"], 0) + 1
    
    # Calculate statistics
    total_tests = len(results)
    avg_response_time = sum(r["response_time_ms"] for r in results if r["status"] == "SUCCESS") / max(success_count, 1)
    avg_delay = sum(r.get("delay_used", 0) for r in results) / total_tests
    
    stats = {
        "test_info": {
            "test_url": TEST_URL,
            "timestamp": datetime.now().isoformat(),
            "total_proxies_tested": total_tests,
            "timeout_seconds": TIMEOUT,
            "concurrency": CONCURRENCY,
            "min_delay": MIN_DELAY,
            "max_delay": MAX_DELAY,
            "anti_detection": True
        },
        "results_summary": {
            "successful": success_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "forbidden": forbidden_count,
            "success_rate_percent": round((success_count / total_tests) * 100, 2) if total_tests > 0 else 0,
            "average_response_time_ms": round(avg_response_time, 2) if success_count > 0 else 0,
            "average_delay_seconds": round(avg_delay, 2),
            "block_types": block_types
        },
        "detailed_results": results
    }
    
    # Save results to JSON
    results_file = output_dir / "test_results_stealth.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    # Generate enhanced summary report
    safe_print("\n" + "=" * 80)
    safe_print("[4/4] ENHANCED TEST RESULTS SUMMARY")
    safe_print("=" * 80)
    safe_print(f"Total Proxies Tested: {total_tests}")
    safe_print(f"✓ Successful:         {success_count:4d} ({(success_count/total_tests)*100:5.1f}%)")
    safe_print(f"✗ Failed:             {failed_count:4d} ({(failed_count/total_tests)*100:5.1f}%)")
    safe_print(f"🚫 Blocked:            {blocked_count:4d} ({(blocked_count/total_tests)*100:5.1f}%)")
    safe_print(f"⛔ Forbidden:          {forbidden_count:4d} ({(forbidden_count/total_tests)*100:5.1f}%)")
    safe_print("-" * 80)
    safe_print(f"Success Rate:         {stats['results_summary']['success_rate_percent']:.1f}%")
    safe_print(f"Avg Response Time:    {stats['results_summary']['average_response_time_ms']:.0f}ms")
    safe_print(f"Avg Request Delay:    {stats['results_summary']['average_delay_seconds']:.1f}s")
    
    if block_types:
        safe_print("\nBlock Type Analysis:")
        for block_type, count in block_types.items():
            safe_print(f"  {block_type}: {count} ({(count/total_tests)*100:.1f}%)")
    
    safe_print("=" * 80)
    
    # File locations
    safe_print(f"\n📁 Results saved to: {output_dir}")
    safe_print(f"📄 Detailed report:   {results_file}")
    safe_print(f"📂 Successful HTMLs:  {output_dir / 'success'}")
    safe_print(f"📝 Content samples:   {output_dir / 'samples'}")
    safe_print(f"🔍 {success_count} HTML files saved from working proxies")
    
    # Working proxies list with enhanced info
    if success_count > 0:
        working_proxies_file = output_dir / "working_proxies_enhanced.txt"
        with open(working_proxies_file, "w", encoding="utf-8") as f:
            f.write(f"# Enhanced Working Proxies - {success_count}/{total_tests} ({(success_count/total_tests)*100:.1f}%)\n")
            f.write(f"# Test URL: {TEST_URL}\n")
            f.write(f"# Test Date: {datetime.now().isoformat()}\n")
            f.write(f"# Anti-Detection Features: Random Headers, Random Cookies, Random Delays\n\n")
            for result in sorted([r for r in results if r["status"] == "SUCCESS"], key=lambda x: x['response_time_ms']):
                f.write(f"{result['proxy_address']} ({result['response_time_ms']}ms, {result['content_length']} bytes)\n")
        safe_print(f"📋 Enhanced proxy list: {working_proxies_file}")
    
    safe_print("\n✨ Enhanced proxy testing completed!")
    return stats

if __name__ == "__main__":
    try:
        asyncio.run(test_all_proxies_stealth())
    except KeyboardInterrupt:
        safe_print("\n⚠️  Test interrupted by user")
    except Exception as e:
        safe_print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
