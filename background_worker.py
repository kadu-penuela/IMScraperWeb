import os
import json
import time
import asyncio
import aiohttp
import logging
import re
from datetime import datetime, timedelta
import base64
from io import BytesIO
from odf.opendocument import OpenDocumentSpreadsheet
from odf.table import Table, TableRow, TableCell
from odf.text import P
from functools import wraps
import signal
import sys

# Configuration
JOBS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jobs')
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

# Ensure directories exist
for directory in [JOBS_DIR, RESULTS_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'worker.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global settings
DEFAULT_TIMEOUT = 15  # Default timeout for API calls in seconds
URL_TIMEOUT = 30      # Maximum time to spend on a single URL
MAX_RETRIES = 1       # Maximum number of retries for API calls

# Rate limiters
class RateLimiter:
    def __init__(self, calls_per_unit, unit_time, name="API"):
        self.calls_per_unit = calls_per_unit
        self.unit_time = unit_time
        self.calls = []
        self.lock = asyncio.Lock()
        self.name = name

    async def wait_if_needed(self):
        async with self.lock:
            now = time.time()
            # Clean old calls
            self.calls = [call_time for call_time in self.calls if now - call_time < self.unit_time]

            if len(self.calls) >= self.calls_per_unit:
                sleep_time = self.calls[0] + self.unit_time - now
                if sleep_time > 0:
                    logging.info(f"{self.name} rate limit reached: waiting {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                self.calls = self.calls[1:]

            self.calls.append(now)

# Initialize rate limiters
ahrefs_limiter = RateLimiter(calls_per_unit=60, unit_time=60, name="Ahrefs")
majestic_limiter = RateLimiter(calls_per_unit=300, unit_time=1, name="Majestic")
dataforseo_limiter = RateLimiter(calls_per_unit=500, unit_time=60, name="DataForSEO")

# Helper functions
def normalize_url(url):
    """Standardize URL format for consistent processing"""
    # Handle URLs without protocol
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url

    # Extract domain
    parsed_url = re.sub(r'^(http://|https://)', '', url.strip().lower())
    domain = parsed_url.split('/')[0].split('?')[0].split('#')[0]

    return 'http://' + domain

def log_memory_usage():
    """Log current memory usage"""
    try:
        # Get memory usage from /proc/self/status on Linux
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if 'VmRSS' in line:
                    mem_usage = line.split()[1]
                    logging.info(f"Memory usage: {int(mem_usage) / 1024:.1f} MB")
                    return
    except Exception:
        pass

# Retry decorator for OS errors
def retry_on_os_error(max_attempts=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except OSError as e:
                    if attempt == max_attempts - 1:
                        raise
                    logging.warning(f"OS write error, attempt {attempt + 1} of {max_attempts}: {e}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# API interaction functions
async def get_majestic_data(session, url, api_key):
    """Fetch topic data from Majestic API"""
    if not api_key:
        return "N/A"

    # Strip out http, https, www, and trailing slashes
    cleaned_url = re.sub(r'^(https?://)?(www\.)?', '', url.strip().lower()).split('/')[0]

    try:
        await majestic_limiter.wait_if_needed()
        api_url = "https://api.majestic.com/api/json"
        params = {
            "app_api_key": api_key,
            "cmd": "GetTopics",
            "datasource": "fresh",
            "Item": cleaned_url,
            "Count": 100,
            "SortOrder": "desc",
        }
        async with session.get(api_url, params=params, timeout=DEFAULT_TIMEOUT) as response:
            if response.status != 200:
                return "N/A"

            data = await response.json()
            if data.get("Code") != "OK":
                return "N/A"

            topics = data.get("DataTables", {}).get("Topics", {}).get("Data", [])
            return topics[0].get("Topic", "N/A") if topics else "N/A"
    except Exception as e:
        logging.warning(f"Majestic API error for {url}: {e}")
        return "N/A"

async def get_ahrefs_data(session, url, api_key):
    """Fetch metrics from Ahrefs API"""
    if not api_key:
        return {"dr": "N/A", "refdomains": "N/A", "traffic": "N/A"}

    try:
        results = {"dr": "N/A", "refdomains": "N/A", "traffic": "N/A"}
        headers = {"Authorization": f"Bearer {api_key}"}

        # Domain Rating
        try:
            await ahrefs_limiter.wait_if_needed()
            async with session.get(
                "https://api.ahrefs.com/v3/site-explorer/domain-rating",
                headers=headers,
                params={"target": url, "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")},
                timeout=DEFAULT_TIMEOUT
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    results["dr"] = data.get("domain_rating", {}).get("domain_rating", "N/A")
        except Exception:
            pass

        # Referring Domains
        try:
            await ahrefs_limiter.wait_if_needed()
            async with session.get(
                "https://api.ahrefs.com/v3/site-explorer/refdomains-history",
                headers=headers,
                params={
                    "target": url,
                    "mode": "domain",
                    "protocol": "both",
                    "date_from": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                },
                timeout=DEFAULT_TIMEOUT
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    results["refdomains"] = data.get("refdomains", "N/A")
        except Exception:
            pass

        # Traffic
        try:
            await ahrefs_limiter.wait_if_needed()
            async with session.get(
                "https://api.ahrefs.com/v3/site-explorer/metrics",
                headers=headers,
                params={
                    "target": url,
                    "mode": "domain",
                    "protocol": "both",
                    "date": datetime.today().replace(day=1).strftime("%Y-%m-%d"),
                },
                timeout=DEFAULT_TIMEOUT
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    results["traffic"] = data.get("metrics", {}).get("org_traffic", "N/A")
        except Exception:
            pass

        return results
    except Exception as e:
        logging.warning(f"Ahrefs API error for {url}: {e}")
        return {"dr": "N/A", "refdomains": "N/A", "traffic": "N/A"}

async def get_dataforseo_data(session, url, api_key):
    """Fetch metrics from DataForSEO API"""
    if not api_key:
        return {"referring_domains": "N/A", "traffic": "N/A", "rank": "N/A"}

    results = {"referring_domains": "N/A", "traffic": "N/A", "rank": "N/A"}

    # API Key parsing
    try:
        login, password = api_key.split(':')
    except ValueError:
        logging.error("Invalid API key format for DataForSEO. Expected 'login:password'")
        return results

    # Headers setup
    headers = {
        'Authorization': f'Basic {base64.b64encode(f"{login}:{password}".encode()).decode()}',
        'Content-Type': 'application/json'
    }

    # Enhanced domain cleaning
    cleaned_domain = re.sub(r'^https?://', '', url, flags=re.IGNORECASE).strip()
    cleaned_domain = re.split(r'[\/?:]', cleaned_domain)[0]  # Split on /, ?, or :
    cleaned_domain = cleaned_domain.lower()

    # Verify domain format
    if not re.match(r'^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$', cleaned_domain):
        return results

    # Backlinks summary endpoint
    backlinks_payload = {
        0: {
            "target": cleaned_domain,
            "include_subdomains": True,
            "backlinks_filters": ["dofollow", "=", True],
            "backlinks_status_type": "live"
        }
    }

    # Traffic payload
    traffic_payload = [{
        "targets": [cleaned_domain],
        "location_name": "United States",
        "language_name": "English"
    }]

    # Backlinks summary request
    try:
        await dataforseo_limiter.wait_if_needed()
        async with session.post(
            "https://sandbox.dataforseo.com/v3/backlinks/summary/live",
            headers=headers,
            json=backlinks_payload,
            timeout=DEFAULT_TIMEOUT
        ) as backlinks_response:
            if backlinks_response.status == 200:
                backlinks_data = await backlinks_response.json()

                if backlinks_data.get('status_code') == 20000 and backlinks_data.get('tasks'):
                    backlinks_result = backlinks_data['tasks'][0]['result'][0]
                    results["referring_domains"] = backlinks_result.get('referring_main_domains', 'N/A')
                    results["rank"] = backlinks_result.get('rank', 'N/A')
    except Exception:
        pass

    # Traffic request
    try:
        await dataforseo_limiter.wait_if_needed()
        async with session.post(
            "https://sandbox.dataforseo.com/v3/dataforseo_labs/google/bulk_traffic_estimation/live",
            headers=headers,
            json=traffic_payload,
            timeout=DEFAULT_TIMEOUT
        ) as traffic_response:
            if traffic_response.status == 200:
                traffic_data = await traffic_response.json()

                if (traffic_data.get('status_code') == 20000 and
                    traffic_data.get('tasks') and
                    traffic_data['tasks'][0].get('result') and
                    traffic_data['tasks'][0]['result'][0].get('items')):

                    # Get the first item's organic etv
                    first_item = traffic_data['tasks'][0]['result'][0]['items'][0]
                    traffic = first_item.get('metrics', {}).get('organic', {}).get('etv', 'N/A')
                    results["traffic"] = traffic
    except Exception:
        pass

    return results

async def check_https(session, url):
    """Check if a site supports HTTPS"""
    try:
        https_url = re.sub(r'^http://', 'https://', url, flags=re.IGNORECASE)
        async with session.get(https_url, timeout=10) as response:
            return "Yes" if response.status == 200 else "No"
    except Exception:
        return "No"

@retry_on_os_error(max_attempts=3, delay=1)
def export_to_ods(results, use_majestic=True, use_ahrefs=True, use_dataforseo=True):
    """Export results to ODS spreadsheet format"""
    doc = OpenDocumentSpreadsheet()
    table = Table(name="Results")

    headers = ["URL", "Status Code"]
    if use_majestic:
        headers.append("Majestic Topics")
    if use_ahrefs:
        headers.extend(["Ahrefs Referring Domains", "Ahrefs Traffic", "Ahrefs Domain Rating"])
    if use_dataforseo:
        headers.extend(["DataForSEO Referring Domains", "DataForSEO Traffic", "DataForSEO Rank"])
    headers.append("HTTPS")

    header_row = TableRow()
    for header in headers:
        cell = TableCell()
        cell.addElement(P(text=header))
        header_row.addElement(cell)
    table.addElement(header_row)

    for result in results:
        row = TableRow()
        for header in headers:
            key_map = {
                "URL": "url",
                "Status Code": "status_code",
                "Majestic Topics": "majestic_topics",
                "Ahrefs Referring Domains": "ahrefs_refdomains",
                "Ahrefs Traffic": "ahrefs_traffic",
                "Ahrefs Domain Rating": "ahrefs_dr",
                "DataForSEO Referring Domains": "dataforseo_referring_domains",
                "DataForSEO Traffic": "dataforseo_traffic",
                "DataForSEO Rank": "dataforseo_rank",
                "HTTPS": "secure"
            }
            value = result.get(key_map[header], "N/A")
            cell = TableCell()
            cell.addElement(P(text=str(value)))
            row.addElement(cell)
        table.addElement(row)

    doc.spreadsheet.addElement(table)
    output = BytesIO()
    doc.save(output)
    output.seek(0)
    return output

async def process_url(session, url, majestic_api_key=None, ahrefs_api_key=None, dataforseo_api_key=None):
    """Process a single URL to gather all metrics with a simple timeout"""
    normalized_url = normalize_url(url)
    logging.info(f"Processing URL: {normalized_url}")

    try:
        # Get status code with a short timeout
        status_code = "N/A"
        try:
            async with session.get(normalized_url, timeout=10) as response:
                status_code = response.status
        except Exception:
            status_code = "N/A"

        # Setup and run tasks in parallel with individual error handling
        secure_result = "N/A"
        majestic_result = "N/A"
        ahrefs_result = {"dr": "N/A", "refdomains": "N/A", "traffic": "N/A"}
        dataforseo_result = {"referring_domains": "N/A", "traffic": "N/A", "rank": "N/A"}

        # HTTPS check
        try:
            secure_result = await check_https(session, normalized_url)
        except Exception:
            secure_result = "N/A"

        # Majestic check
        if majestic_api_key:
            try:
                majestic_result = await get_majestic_data(session, normalized_url, majestic_api_key)
            except Exception:
                majestic_result = "N/A"

        # Ahrefs check
        if ahrefs_api_key:
            try:
                ahrefs_result = await get_ahrefs_data(session, normalized_url, ahrefs_api_key)
            except Exception:
                ahrefs_result = {"dr": "N/A", "refdomains": "N/A", "traffic": "N/A"}

        # DataForSEO check
        if dataforseo_api_key:
            try:
                dataforseo_result = await get_dataforseo_data(session, normalized_url, dataforseo_api_key)
            except Exception:
                dataforseo_result = {"referring_domains": "N/A", "traffic": "N/A", "rank": "N/A"}

        # Build the result dictionary
        result = {
            "url": normalized_url,
            "status_code": status_code,
            "secure": secure_result
        }

        # Add API results
        if majestic_api_key:
            result["majestic_topics"] = majestic_result

        if ahrefs_api_key:
            result.update({
                "ahrefs_dr": ahrefs_result["dr"],
                "ahrefs_refdomains": ahrefs_result["refdomains"],
                "ahrefs_traffic": ahrefs_result["traffic"]
            })

        if dataforseo_api_key:
            result.update({
                "dataforseo_referring_domains": dataforseo_result["referring_domains"],
                "dataforseo_traffic": dataforseo_result["traffic"],
                "dataforseo_rank": dataforseo_result["rank"]
            })

        return result

    except Exception as e:
        logging.warning(f"Error processing URL {normalized_url}: {e}")
        # Return a result with N/A values
        result = {
            "url": normalized_url,
            "status_code": "N/A",
            "secure": "N/A"
        }

        if majestic_api_key:
            result["majestic_topics"] = "N/A"

        if ahrefs_api_key:
            result.update({
                "ahrefs_dr": "N/A",
                "ahrefs_refdomains": "N/A",
                "ahrefs_traffic": "N/A"
            })

        if dataforseo_api_key:
            result.update({
                "dataforseo_referring_domains": "N/A",
                "dataforseo_traffic": "N/A",
                "dataforseo_rank": "N/A"
            })

        return result

async def process_url_with_timeout(session, url, majestic_api_key=None, ahrefs_api_key=None, dataforseo_api_key=None):
    """Wrapper to process URL with overall timeout"""
    try:
        return await asyncio.wait_for(
            process_url(session, url, majestic_api_key, ahrefs_api_key, dataforseo_api_key),
            timeout=URL_TIMEOUT
        )
    except asyncio.TimeoutError:
        logging.warning(f"Timeout processing URL: {url}")
        # Return a result with N/A values
        result = {
            "url": url,
            "status_code": "Timeout",
            "secure": "N/A"
        }

        if majestic_api_key:
            result["majestic_topics"] = "N/A"

        if ahrefs_api_key:
            result.update({
                "ahrefs_dr": "N/A",
                "ahrefs_refdomains": "N/A",
                "ahrefs_traffic": "N/A"
            })

        if dataforseo_api_key:
            result.update({
                "dataforseo_referring_domains": "N/A",
                "dataforseo_traffic": "N/A",
                "dataforseo_rank": "N/A"
            })

        return result

async def process_job(job_id, job_data):
    """Process each URL one by one, never getting stuck"""
    start_time = time.time()
    urls = job_data.get('urls', [])
    use_majestic = job_data.get('use_majestic', False)
    use_ahrefs = job_data.get('use_ahrefs', False)
    use_dataforseo = job_data.get('use_dataforseo', False)

    majestic_api_key = job_data.get('majestic_api_key') if use_majestic else None
    ahrefs_api_key = job_data.get('ahrefs_api_key') if use_ahrefs else None
    dataforseo_api_key = job_data.get('dataforseo_api_key') if use_dataforseo else None

    logging.info(f"Starting job {job_id} with {len(urls)} URLs")
    logging.info(f"APIs enabled: Majestic={use_majestic}, Ahrefs={use_ahrefs}, DataForSEO={use_dataforseo}")

    # Create status file to indicate job is processing
    with open(os.path.join(JOBS_DIR, f"{job_id}.status"), 'w') as f:
        json.dump({"status": "processing", "progress": 0, "total": len(urls)}, f)

    try:
        # Create session with reasonable limits
        connector = aiohttp.TCPConnector(limit=20)  # Limit concurrent connections
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=15)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            results = []

            # Process URLs one by one, ensuring we never get stuck
            for i, url in enumerate(urls):
                try:
                    # Process this URL with a timeout
                    result = await process_url_with_timeout(
                        session,
                        url,
                        majestic_api_key,
                        ahrefs_api_key,
                        dataforseo_api_key
                    )
                    results.append(result)
                except Exception as e:
                    logging.error(f"Unhandled error processing URL {url}: {e}")
                    # Create a placeholder result with N/A values
                    error_result = {
                        "url": url,
                        "status_code": "Error",
                        "secure": "N/A"
                    }

                    if use_majestic:
                        error_result["majestic_topics"] = "N/A"

                    if use_ahrefs:
                        error_result.update({
                            "ahrefs_dr": "N/A",
                            "ahrefs_refdomains": "N/A",
                            "ahrefs_traffic": "N/A"
                        })

                    if use_dataforseo:
                        error_result.update({
                            "dataforseo_referring_domains": "N/A",
                            "dataforseo_traffic": "N/A",
                            "dataforseo_rank": "N/A"
                        })

                    results.append(error_result)

                # Update progress after each URL
                progress = i + 1
                elapsed = time.time() - start_time
                avg_time_per_url = elapsed / progress
                remaining_estimate = avg_time_per_url * (len(urls) - progress)

                with open(os.path.join(JOBS_DIR, f"{job_id}.status"), 'w') as f:
                    json.dump({
                        "status": "processing",
                        "progress": progress,
                        "total": len(urls),
                        "elapsed_seconds": elapsed,
                        "estimated_remaining_seconds": remaining_estimate
                    }, f)

                # Log progress periodically
                if progress % 10 == 0 or progress == len(urls):
                    logging.info(f"Job {job_id}: Processed {progress}/{len(urls)} URLs")
                    logging.info(f"Average time per URL: {avg_time_per_url:.2f}s")
                    logging.info(f"Estimated time remaining: {remaining_estimate/60:.1f} minutes")
                    log_memory_usage()

            # Create ODS file
            logging.info(f"Creating ODS export for job {job_id} with {len(results)} results")
            output = export_to_ods(results, use_majestic, use_ahrefs, use_dataforseo)

            # Save result file
            result_path = os.path.join(RESULTS_DIR, f"{job_id}.ods")
            with open(result_path, 'wb') as f:
                f.write(output.getvalue())

            # Calculate stats for logging
            total_duration = time.time() - start_time
            successful_urls = sum(1 for r in results if r.get('status_code') not in ['Timeout', 'Error', 'N/A'])
            error_urls = len(results) - successful_urls

            # Update status file to indicate completion
            with open(os.path.join(JOBS_DIR, f"{job_id}.status"), 'w') as f:
                json.dump({
                    "status": "completed",
                    "progress": len(results),
                    "total": len(urls),
                    "successful": successful_urls,
                    "errors": error_urls,
                    "duration_seconds": total_duration,
                    "avg_time_per_url": total_duration / len(urls) if len(urls) > 0 else 0,
                    "download_url": f"/download_result/{job_id}"
                }, f)

            logging.info(f"Job {job_id} completed successfully in {total_duration:.2f}s " +
                         f"({successful_urls}/{len(urls)} successful)")

    except Exception as e:
        logging.error(f"Error processing job {job_id}: {e}", exc_info=True)
        # Update status file to indicate error
        with open(os.path.join(JOBS_DIR, f"{job_id}.status"), 'w') as f:
            json.dump({
                "status": "error",
                "error": str(e),
                "progress": len(results) if 'results' in locals() else 0,
                "total": len(urls)
            }, f)

async def main():
    """Main function to continuously watch for and process jobs"""
    logging.info("Background worker started")

    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        logging.info("Shutting down background worker")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Cleanup any stale status files (from previous crashes)
    for filename in os.listdir(JOBS_DIR):
        if filename.endswith('.status'):
            job_id = filename.split('.')[0]
            if not os.path.exists(os.path.join(JOBS_DIR, f"{job_id}.json")):
                os.remove(os.path.join(JOBS_DIR, filename))

    # Main loop
    last_health_log = 0
    while True:
        try:
            # Periodically log health status
            current_time = time.time()
            if current_time - last_health_log > 600:  # Every 10 minutes
                logging.info("Worker heartbeat: still running")
                log_memory_usage()
                last_health_log = current_time

            # Look for new job files
            for filename in os.listdir(JOBS_DIR):
                if filename.endswith('.json'):
                    job_id = filename.split('.')[0]
                    job_file = os.path.join(JOBS_DIR, filename)
                    status_file = os.path.join(JOBS_DIR, f"{job_id}.status")

                    # Skip jobs that are already being processed
                    if os.path.exists(status_file):
                        continue

                    # Load job data
                    try:
                        with open(job_file, 'r') as f:
                            job_data = json.load(f)

                        # Process the job
                        await process_job(job_id, job_data)

                    except json.JSONDecodeError:
                        logging.error(f"Invalid JSON in job file: {job_file}")
                        os.rename(job_file, f"{job_file}.error")
                    except Exception as e:
                        logging.error(f"Error processing job file {job_file}: {e}", exc_info=True)

            # Clean up old completed jobs (older than 24 hours)
            current_time = time.time()
            for filename in os.listdir(JOBS_DIR):
                if filename.endswith('.status'):
                    file_path = os.path.join(JOBS_DIR, filename)
                    file_age = current_time - os.path.getmtime(file_path)

                    # If file is older than 24 hours
                    if file_age > 86400:
                        try:
                            with open(file_path, 'r') as f:
                                status_data = json.load(f)

                            if status_data.get('status') in ['completed', 'error', 'cancelled']:
                                job_id = filename.split('.')[0]
                                result_file = os.path.join(RESULTS_DIR, f"{job_id}.ods")
                                job_file = os.path.join(JOBS_DIR, f"{job_id}.json")

                                # Remove files if they exist
                                for f in [result_file, job_file, file_path]:
                                    if os.path.exists(f):
                                        os.remove(f)

                        except Exception as e:
                            logging.error(f"Error cleaning up old job {filename}: {e}")

  # Sleep to avoid excessive CPU usage
            await asyncio.sleep(5)

        except Exception as e:
            logging.error(f"Error in main loop: {e}", exc_info=True)
            await asyncio.sleep(10)  # Longer sleep on error

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"Fatal error in worker: {e}", exc_info=True)
        sys.exit(1)