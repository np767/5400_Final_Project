"""
Speech Collector v2 - Batch Mode
Collects bipartisan_and_other_speeches from official member sites.
Processes members one by one, 50 House + 50 Senate, 30 speeches each was run 
    for efficiency and setting size balance.
"""
import re
import json
import time
import random
import urllib.parse
import logging
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dateparser

# Setup logging
def setup_logging(log_file: Optional[str] = None):
    """Configure logging to both file and console"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    handlers = [logging.StreamHandler()]
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding='utf-8'))
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    return logging.getLogger(__name__)

CONFIG = {
    "user_agent": "DSAN-5400-SpeechCollector/1.1 (+for research; contact: vt216@georgetown.edu)",
    "sleep_min": 0.5,
    "sleep_max": 1.0,
    "timeout": 20,
    "max_speeches_per_person": 30,
}

session = requests.Session()
session.headers.update({"User-Agent": CONFIG["user_agent"]})

def polite_get(url: str):
    """Fetch URL with rate limiting."""
    try:
        time.sleep(random.uniform(CONFIG["sleep_min"], CONFIG["sleep_max"]))
        resp = session.get(url, timeout=CONFIG["timeout"], allow_redirects=True)
        if resp.status_code == 200:
            return resp
    except:
        pass
    return None

def extract_title_and_date(html: str) -> tuple:
    """Extract title and date from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    
    title = ""
    if soup.title:
        title = soup.title.get_text(" ", strip=True)
    elif soup.find("h1"):
        title = soup.find("h1").get_text(" ", strip=True)
    
    date_iso = None
    for time_tag in soup.find_all("time"):
        dt = time_tag.get("datetime") or time_tag.get("content") or time_tag.get_text(" ", strip=True)
        parsed = dateparser.parse(dt) if dt else None
        if parsed:
            date_iso = parsed.isoformat()
            break
    
    return title, date_iso

def infer_year(date_iso: Optional[str], url: str, title: str) -> Optional[int]:
    """Infer year from date, URL, or title."""
    if date_iso:
        match = re.search(r'(\d{4})', date_iso)
        if match:
            return int(match.group(1))
    
    match = re.search(r'/(\d{4})/', url)
    if match:
        return int(match.group(1))
    
    match = re.search(r'\b(20\d{2})\b', title)
    if match:
        return int(match.group(1))
    
    return None

def slugify(text: str) -> str:
    """Convert text to filename-safe slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text[:50]

def infer_filename(title: str, date_iso: Optional[str], url: str) -> str:
    """Generate filename: bipartisan_YYYY_short-title.txt"""
    year = infer_year(date_iso, url, title) or "unknown"
    title_slug = slugify(title)[:40] if title else "speech"
    return f"bipartisan_{year}_{title_slug}.txt"

def slugify_name(name: str) -> str:
    """Convert person name to JSON key format ('Mitch McConnell' -> 'mitch_mcconnell')."""
    name = name.lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[-\s]+', '_', name)
    return name

def find_speech_urls_from_site(base_url: str) -> List[str]:
    """Find speech URLs by checking common patterns on official sites."""
    speech_urls = []
    
    # Common speech section URLs
    common_paths = [
        "/public/index.cfm/pressreleases",
        "/public/index.cfm/speeches",
        "/public/index.cfm/statements",
        "/newsroom",
        "/press-releases",
        "/press-release",
        "/speeches",
        "/statements",
        "/newsroom/press-releases",
        "/newsroom/speeches",
        "/media/press-releases",
    ]
    
    for path in common_paths:
        url = urllib.parse.urljoin(base_url, path)
        resp = polite_get(url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select("a[href]"):
                href = a.get("href")
                if not href:
                    continue
                full_url = urllib.parse.urljoin(url, href)
                href_lower = href.lower()
                text_lower = (a.get_text(" ", strip=True) or "").lower()
                
                # Look for speech-like URLs
                is_speech_link = (
                    any(x in href_lower for x in [
                        "speech", "remarks", "statement", "press-release", 
                        "documentsingle", "/202", "/2021", "/2022", "/2023", "/2024", "/2025"
                    ]) or
                    any(x in text_lower for x in [
                        "speech", "remarks", "statement", "address"
                    ])
                )
                
                if is_speech_link:
                    # Exclude listing pages
                    if not any(x in href_lower for x in ["/list", "/archive", "/all", "/page/", "/category/", "?page="]):
                        if full_url not in speech_urls:
                            speech_urls.append(full_url)
    
    return speech_urls

def collect_speeches_from_urls(urls: List[str]) -> Dict[str, str]:
    """Collect bipartisan speeches from a list of URLs."""
    speeches = {}
    
    for url in urls[:CONFIG["max_speeches_per_person"] * 2]:
        if len(speeches) >= CONFIG["max_speeches_per_person"]:
            break
            
        resp = polite_get(url)
        if not resp:
            continue
        
        is_pdf = url.lower().endswith(".pdf") or "application/pdf" in resp.headers.get("Content-Type", "").lower()
        
        if is_pdf:
            title = url.split("/")[-1].replace(".pdf", "").replace("-", " ").replace("_", " ")
            html = ""
            date_iso = None
        else:
            html = resp.text
            title, date_iso = extract_title_and_date(html)
        
        # Check if it's actually a speech (has content)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            
            # Exclude error pages and listing pages
            if "404" in (title or "").lower() or "error" in (title or "").lower():
                continue
            
            url_lower = url.lower()
            if any(x in url_lower for x in ["/list", "/archive", "/all", "/page/", "/category/", "?page=", "audiostatements", "videostatements"]):
                if not re.search(r'/\d{4}/', url_lower) and not re.search(r'/\d+$', url_lower):
                    continue
            
            for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                tag.decompose()
            text = soup.get_text(" ", strip=True)
            if len(text.strip()) < 200:
                continue
        
        # Generate filename and add
        filename = infer_filename(title, date_iso, url)
        
        if filename not in speeches:
            speeches[filename] = url
    
    return speeches

def discover_house_member_sites() -> pd.DataFrame:
    """Discover House member official websites - only actual members."""
    HOUSE_DIR = "https://www.house.gov/representatives"
    resp = polite_get(HOUSE_DIR)
    out = []
    
    if not resp:
        logging.warning("Could not fetch House directory")
        return pd.DataFrame(columns=["name", "site_url", "chamber"])
    
    soup = BeautifulSoup(resp.text, "html.parser")
    seen_urls = set()
    
    # Look for member links - they typically have names and .house.gov URLs
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        text = (a.get_text(" ", strip=True) or "").strip()
        
        if not href or not text or len(text) < 3:
            continue
        
        if not href.startswith("http"):
            href = urllib.parse.urljoin(HOUSE_DIR, href)
        
        href_lower = href.lower()
        
        # Must be a .house.gov domain
        if ".house.gov" not in href_lower:
            continue
        
        # Exclude institutional pages
        exclude_patterns = [
            "house.gov/", "www.house.gov", "/representatives", "/committees",
            "/leadership", "/history", "/hearings", "/membership", "/states"
        ]
        if any(pattern in href_lower for pattern in exclude_patterns):
            continue
        
        # Must look like a member page (has name-like structure in URL)
        # Member URLs typically: lastname.house.gov or firstname-lastname.house.gov
        domain_part = href_lower.split("//")[1].split("/")[0] if "//" in href_lower else ""
        if ".house.gov" in domain_part:
            # Extract subdomain
            subdomain = domain_part.split(".house.gov")[0]
            # Should have letters (name-like), not just numbers or generic words
            if subdomain and subdomain.replace("-", "").replace("_", "").isalpha() and len(subdomain) > 2:
                if href not in seen_urls:
                    # Text should look like a name
                    if len(text.split()) >= 1 and any(c.isalpha() for c in text):
                        out.append({
                            "name": text,
                            "site_url": href,
                            "chamber": "house"
                        })
                        seen_urls.add(href)
    
    df = pd.DataFrame(out).drop_duplicates(subset=["site_url"])
    df = df[df["name"].str.len() > 2]
    return df

def discover_senate_member_sites() -> pd.DataFrame:
    """Discover Senate member official websites - only actual members."""
    SENATE_DIR = "https://www.senate.gov/senators/index.htm"
    resp = polite_get(SENATE_DIR)
    out = []
    
    if not resp:
        logging.warning("Could not fetch Senate directory")
        return pd.DataFrame(columns=["name", "site_url", "chamber"])
    
    soup = BeautifulSoup(resp.text, "html.parser")
    seen_urls = set()
    
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        txt = (a.get_text(" ", strip=True) or "").strip()
        
        if not href or not txt or len(txt) < 3:
            continue
        
        if not href.startswith("http"):
            href = urllib.parse.urljoin(SENATE_DIR, href)
        
        href_lower = href.lower()
        
        # Must be a .senate.gov domain
        if ".senate.gov" not in href_lower:
            continue
        
        # Exclude institutional pages
        skip_patterns = [
            "/senators/index", "/senators/", "/legislative/", "/about/",
            "/contact/", "/artandhistory/", "/visitors/", "/reference/",
            "www.senate.gov", "/committees", "/leadership", "/history"
        ]
        if any(pattern in href_lower for pattern in skip_patterns):
            continue
        
        # Must look like a member page (has name-like structure)
        # Member URLs typically: lastname.senate.gov or www.lastname.senate.gov
        domain_part = href_lower.split("//")[1].split("/")[0] if "//" in href_lower else ""
        if ".senate.gov" in domain_part:
            subdomain = domain_part.replace("www.", "").split(".senate.gov")[0]
            # Should have letters (name-like)
            if subdomain and subdomain.replace("-", "").replace("_", "").isalpha() and len(subdomain) > 2:
                if href not in seen_urls:
                    # Text should look like a name
                    if len(txt.split()) >= 1 and any(c.isalpha() for c in txt):
                        out.append({
                            "name": txt,
                            "site_url": href,
                            "chamber": "senate"
                        })
                        seen_urls.add(href)
    
    df = pd.DataFrame(out).drop_duplicates(subset=["site_url"])
    df = df[df["name"].str.len() > 2]
    return df

def process_member(site_url: str, name: str) -> Dict[str, str]:
    """Process a single member to collect speeches."""
    speech_urls = find_speech_urls_from_site(site_url)
    speeches = collect_speeches_from_urls(speech_urls)
    return speeches

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch speech collector for bipartisan speeches")
    parser.add_argument("--limit", type=int, default=50, help="Number of members per chamber (default: 50)")
    parser.add_argument("--max-per-person", type=int, default=30, help="Max speeches per person (default: 30)")
    parser.add_argument("--output", type=str, default="../../data/config/collected_speeches.json", help="Output JSON file")
    parser.add_argument("--log-file", type=str, default=None, help="Log file path (optional)")
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    
    CONFIG["max_speeches_per_person"] = args.max_per_person
    
    logger.info("=" * 60)
    logger.info("Simple Speech Collector v2 - Batch Mode")
    logger.info("=" * 60)
    logger.info("Collecting bipartisan_and_other_speeches")
    logger.info(f"Target: {args.limit} House + {args.limit} Senate members")
    logger.info(f"Max {args.max_per_person} speeches per person")
    
    # Load existing JSON to use as template and avoid duplicates
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists():
        try:
            output = json.loads(output_path.read_text(encoding="utf-8"))
            logger.info(f"Loaded existing JSON with {len(output)} people")
        except:
            output = {}
    else:
        output = {}
    
    # Discover members
    logger.info("[1/4] Discovering member sites...")
    house_df = discover_house_member_sites()
    senate_df = discover_senate_member_sites()
    logger.info(f"House sites: {len(house_df)}")
    logger.info(f"Senate sites: {len(senate_df)}")
    
    # Limit members
    house_members = house_df.head(args.limit)
    senate_members = senate_df.head(args.limit)
    all_members = pd.concat([house_members, senate_members], ignore_index=True)
    
    logger.info(f"[2/4] Processing {len(all_members)} members...")
    
    # Process each member
    total_new_speeches = 0
    processed_count = 0
    
    for idx, row in tqdm(all_members.iterrows(), total=len(all_members), desc="Processing members"):
        name = row["name"]
        site_url = row["site_url"]
        
        person_key = slugify_name(name)
        
        # Skip if this looks like an institutional page (not a person name)
        if not person_key or len(person_key) < 3:
            continue
        
        # Skip common non-person keys
        skip_keys = {
            "skip", "housegov", "states", "committees", "representatives",
            "leadership", "membership", "hearings", "history", "main", "content"
        }
        if any(skip in person_key for skip in skip_keys):
            continue
        
        # Initialize person if needed
        if person_key not in output:
            output[person_key] = {}
        if "bipartisan_and_other_speeches" not in output[person_key]:
            output[person_key]["bipartisan_and_other_speeches"] = {}
        
        # Collect speeches
        try:
            speeches = process_member(site_url, name)
            
            # Merge speeches (don't overwrite existing)
            for filename, url in speeches.items():
                if filename not in output[person_key]["bipartisan_and_other_speeches"]:
                    output[person_key]["bipartisan_and_other_speeches"][filename] = url
                    total_new_speeches += 1
            
            # Remove empty categories
            output[person_key] = {k: v for k, v in output[person_key].items() if v}
            
            processed_count += 1
        except Exception as e:
            logger.error(f"Error processing {name}: {e}")
            continue
        
        # Save periodically (every 10 members)
        if (idx + 1) % 10 == 0:
            output_path.write_text(json.dumps(output, indent=4), encoding="utf-8")
    
    # Clean up: remove any non-person entries
    logger.info("[3/4] Cleaning up non-person entries...")
    cleaned_output = {}
    for key, value in output.items():
        if not any(skip in key for skip in skip_keys):
            cleaned_output[key] = value
    output = cleaned_output
    
    # Final save
    logger.info("[4/4] Saving results...")
    output_path.write_text(json.dumps(output, indent=4), encoding="utf-8")
    
    # Summary
    logger.info("Summary:")
    total_people = len([p for p in output.values() if "bipartisan_and_other_speeches" in p and p["bipartisan_and_other_speeches"]])
    total_speeches_final = sum(
        len(p.get("bipartisan_and_other_speeches", {}))
        for p in output.values()
    )
    logger.info(f"People processed: {processed_count}")
    logger.info(f"People with speeches: {total_people}")
    logger.info(f"Total speeches: {total_speeches_final}")
    logger.info(f"New speeches added this run: {total_new_speeches}")
    logger.info(f"Saved to: {output_path}")
    
    return output

if __name__ == "__main__":
    main()
