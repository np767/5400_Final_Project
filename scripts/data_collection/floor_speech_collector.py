"""
Floor Speech Collector - GovInfo Congressional Record API
Collects actual senate/house floor speeches from Congressional Record.
Uses govinfo.gov CREC (Congressional Record) API for verbatim floor remarks.
Writes to separate JSON to avoid overwriting bipartisan speeches.
"""
import os
import re
import json
import time
import random
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

CONFIG = {
    "api_key": "n9QGDwIkhtU1PWyuhCWMh6GSHK3Wyd95MEVvld4s",
    "govinfo_base": "https://api.govinfo.gov",
    "sleep_min": 0.3,
    "sleep_max": 0.7,
    "timeout": 20,
    "max_speeches_per_person": 30,
    "date_start": "2022-01-01",  # Start from recent congress
    "date_end": None,  # Will be set to today
}

session = requests.Session()
session.headers.update({
    "X-Api-Key": CONFIG["api_key"],
    "User-Agent": "DSAN-5400-FloorSpeechCollector/2.0 (research; vt216@georgetown.edu)"
})

def polite_get(url: str, use_api_key: bool = True) -> Optional[requests.Response]:
    """Fetch URL with rate limiting and optional API key."""
    try:
        time.sleep(random.uniform(CONFIG["sleep_min"], CONFIG["sleep_max"]))
        
        # Use API key for api.govinfo.gov, but not for www.govinfo.gov content
        if use_api_key and "api.govinfo.gov" in url:
            resp = session.get(url, timeout=CONFIG["timeout"])
        else:
            # Direct content fetch without API key
            resp = requests.get(url, timeout=CONFIG["timeout"], headers={
                "User-Agent": "DSAN-5400-FloorSpeechCollector/2.0 (research; vt216@georgetown.edu)"
            })
        
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 429:
            print("  Rate limited, waiting 60s...")
            time.sleep(60)
            return polite_get(url, use_api_key)
        else:
            if resp.status_code != 404:  # Don't log 404s (normal for missing days)
                print(f"  Status {resp.status_code} for {url}")
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
    return None

def slugify_name(name: str) -> str:
    """Convert person name to JSON key format ('Bernie Sanders' -> 'bernie_sanders')."""
    name = name.lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[-\s]+', '_', name)
    return name

def load_existing_people(json_path: str) -> Set[str]:
    """Load existing people from collected_speeches.json for overlap check."""
    try:
        if Path(json_path).exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.keys())
    except Exception as e:
        print(f"Warning: Could not load existing people: {e}")
    return set()

def build_member_lookup(existing_people: Set[str]) -> Dict[str, str]:
    """
    Build lookup dict for matching CREC speaker names to our keys.
    Maps various formats: "Mr. SANDERS", "SANDERS", "Bernie Sanders" -> "bernie_sanders"
    """
    lookup = {}
    
    # Common patterns in Congressional Record
    for person_key in existing_people:
        # Convert key back to name parts
        name_parts = person_key.split('_')
        
        # Common CREC formats
        last_name = name_parts[-1].upper()
        lookup[last_name] = person_key
        lookup[f"Mr. {last_name}"] = person_key
        lookup[f"Ms. {last_name}"] = person_key
        lookup[f"Mrs. {last_name}"] = person_key
        lookup[f"Senator {last_name}"] = person_key
        lookup[f"Representative {last_name}"] = person_key
        
        # Also add the original key
        lookup[person_key] = person_key
    
    return lookup

def get_crec_packages(start_date: str, end_date: str, max_results: int = 100) -> List[Dict]:
    """
    Get Congressional Record packages by constructing package IDs directly.
    GovInfo packages follow format: CREC-YYYY-MM-DD
    """
    packages = []
    
    print(f"  Generating CREC package IDs from {start_date} to {end_date}...")
    
    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Generate all dates in range (excluding weekends - Congress rarely meets)
    current = start
    while current <= end and len(packages) < max_results:
        # Skip weekends (5=Saturday, 6=Sunday)
        if current.weekday() < 5:
            # Construct package ID (format: CREC-YYYY-MM-DD)
            package_id = f"CREC-{current.strftime('%Y-%m-%d')}"
            
            # Check if this package exists by trying to fetch its summary
            url = f"{CONFIG['govinfo_base']}/packages/{package_id}/summary"
            resp = polite_get(url)
            
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                    packages.append({
                        "packageId": package_id,
                        "title": data.get("title", ""),
                        "dateIssued": current.strftime("%Y-%m-%d"),
                        "packageLink": f"https://www.govinfo.gov/app/details/{package_id}"
                    })
                except:
                    pass
        
        current += timedelta(days=1)
    
    print(f"  Found {len(packages)} CREC daily issues")
    return packages

def get_package_summary(package_id: str) -> Optional[Dict]:
    """Get summary/metadata for a specific CREC package."""
    url = f"{CONFIG['govinfo_base']}/packages/{package_id}/summary"
    
    resp = polite_get(url)
    if not resp:
        return None
    
    try:
        return resp.json()
    except:
        return None

def extract_speakers_from_html(html_content: str, member_lookup: Dict[str, str]) -> List[Dict]:
    """
    Parse CREC HTML content to extract floor speeches by speaker.
    Returns list of {speaker_key, text_snippet, full_text}.
    """
    speeches = []
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove script, style tags
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    
    # Get all text
    text = soup.get_text(" ", strip=False)
    
    # Split by common speaker patterns in Congressional Record
    # Pattern: "Mr. LASTNAME." or "Ms. LASTNAME." at start of paragraph
    speaker_pattern = r'\n\s*((?:Mr\.|Ms\.|Mrs\.|Senator|Representative)\s+[A-Z]+\.)'
    
    segments = re.split(speaker_pattern, text)
    
    # Process segments (pairs of speaker + text)
    for i in range(1, len(segments), 2):
        if i + 1 >= len(segments):
            break
        
        speaker_text = segments[i].strip()
        content = segments[i + 1].strip()
        
        # Try to match speaker to our member lookup
        for speaker_variant, person_key in member_lookup.items():
            if speaker_variant.upper() in speaker_text.upper():
                # Found a match!
                if len(content) > 100:  # Minimum speech length
                    speeches.append({
                        "person_key": person_key,
                        "speaker_text": speaker_text,
                        "content": content[:500],  # First 500 chars for snippet
                        "full_length": len(content)
                    })
                break
    
    return speeches

def process_crec_package(package_id: str, member_lookup: Dict[str, str], current_counts: Dict[str, int], max_per_person: int, debug: bool = False) -> Dict[str, List[Dict]]:
    """
    Process a single CREC package to extract floor speeches.
    Returns dict of {person_key: [speech_dicts]}.
    current_counts: dict tracking how many speeches each person already has
    max_per_person: stop collecting for a person once they reach this limit
    """
    speeches_by_person = {}
    people_at_limit = set(k for k, v in current_counts.items() if v >= max_per_person)
    
    # Get package summary to find HTML granules
    summary = get_package_summary(package_id)
    if not summary:
        if debug:
            print(f"      No summary for {package_id}")
        return speeches_by_person
    
    # Try to get HTML content
    # GovInfo provides different formats - try to get HTML or TXT
    granules_url = summary.get("granulesLink")
    if not granules_url:
        if debug:
            print(f"      No granules link in summary")
        return speeches_by_person
    
    # Get granules list
    resp = polite_get(granules_url)
    if not resp:
        if debug:
            print(f"      Failed to fetch granules")
        return speeches_by_person
    
    try:
        granules_data = resp.json()
        granules = granules_data.get("granules", [])
        
        if debug:
            print(f"      Found {len(granules)} granules")
        
        # Process each granule (typically sections like "Senate", "House")
        for granule in granules[:30]:  # Limit to first 30 granules per day
            granule_id = granule.get("granuleId")
            title = granule.get("title", "")
            
            # Focus on Senate and House sections (but process all text content)
            # We'll filter by chamber after extraction
            
            # Construct HTML URL (GovInfo doesn't provide direct htmlLink in API response)
            # Format: https://www.govinfo.gov/content/pkg/{packageId}/html/{granuleId}.htm
            html_link = f"https://www.govinfo.gov/content/pkg/{package_id}/html/{granule_id}.htm"
            
            # Determine chamber from title or granule ID
            chamber = "senate" if "SENATE" in title.upper() or "-PgS" in granule_id else "house"
            
            # Try to get HTML content (don't use API key for content URLs)
            if html_link:
                html_resp = polite_get(html_link, use_api_key=False)
                if html_resp:
                    if debug:
                        print(f"        Processing {chamber} section, HTML length: {len(html_resp.text)}")
                    
                    # Extract speeches from this section
                    speeches = extract_speakers_from_html(html_resp.text, member_lookup)
                    
                    if debug:
                        print(f"        Extracted {len(speeches)} speeches from {granule_id}")
                    
                    for speech in speeches:
                        person_key = speech["person_key"]
                        
                        # Skip if person already at limit
                        if person_key in people_at_limit:
                            continue
                        
                        # Check current count for this person
                        current = current_counts.get(person_key, 0)
                        if current >= max_per_person:
                            people_at_limit.add(person_key)
                            continue
                        
                        if person_key not in speeches_by_person:
                            speeches_by_person[person_key] = []
                        
                        speeches_by_person[person_key].append({
                            "packageId": package_id,
                            "granuleId": granule_id,
                            "chamber": chamber,
                            "date": summary.get("dateIssued"),
                            "url": f"https://www.govinfo.gov/content/pkg/{package_id}/html/{granule_id}.htm",
                            "snippet": speech["content"],
                            "length": speech["full_length"]
                        })
    except Exception as e:
        print(f"  Error processing granules for {package_id}: {e}")
    
    return speeches_by_person

def test_api_detailed():
    """Detailed API test to debug what's actually happening."""
    print("\n" + "=" * 60)
    print("DETAILED API TEST")
    print("=" * 60)
    
    # Test 1: Try to fetch a specific known CREC package
    print("\n[Test 1] Testing direct package access...")
    # Use a known date when Congress was in session (Feb 2024)
    test_package_id = "CREC-2024-02-01"
    url = f"{CONFIG['govinfo_base']}/packages/{test_package_id}/summary"
    print(f"Package ID: {test_package_id}")
    print(f"URL: {url}")
    
    resp = polite_get(url)
    if resp and resp.status_code == 200:
        data = resp.json()
        print(f"✓ Package found! Keys: {list(data.keys())}")
        print(f"  Title: {data.get('title')}")
        print(f"  Date: {data.get('dateIssued')}")
        print(f"  Granules link: {data.get('granulesLink')}")
        
        # Test 2: Get granules
        granules_url = data.get('granulesLink')
        if granules_url:
            print(f"\n[Test 2] Getting granules...")
            print(f"URL: {granules_url}")
            
            granules_resp = polite_get(granules_url)
            if granules_resp and granules_resp.status_code == 200:
                granules_data = granules_resp.json()
                print(f"✓ Granules data keys: {list(granules_data.keys())}")
                
                if "granules" in granules_data:
                    granules = granules_data["granules"]
                    print(f"✓ Found {len(granules)} granules")
                    
                    # Show first few granules
                    for i, g in enumerate(granules[:3]):
                        print(f"\n  Granule {i+1}:")
                        print(f"    ID: {g.get('granuleId')}")
                        print(f"    Title: {g.get('title')}")
                        
                        # Construct HTML URL (not provided in API response)
                        granule_id = g.get('granuleId')
                        html_url = f"https://www.govinfo.gov/content/pkg/{test_package_id}/html/{granule_id}.htm"
                        print(f"    Constructed HTML: {html_url}")
                        
                        # Try to fetch HTML from first granule with text
                        if i == 0:
                            print(f"\n[Test 3] Fetching HTML content sample...")
                            html_resp = polite_get(html_url, use_api_key=False)
                            if html_resp and html_resp.status_code == 200:
                                html_text = html_resp.text
                                print(f"✓ HTML length: {len(html_text)} chars")
                                print(f"✓ Sample (first 500 chars):")
                                print(html_text[:500])
                                
                                # Try to find speaker patterns
                                speaker_matches = re.findall(r'((?:Mr\.|Ms\.|Mrs\.|Senator)\s+[A-Z]+)', html_text[:2000])
                                print(f"\n✓ Found speaker patterns: {speaker_matches[:5]}")
                            else:
                                print(f"✗ Failed to fetch HTML (status: {html_resp.status_code if html_resp else 'None'})")
                else:
                    print("✗ No 'granules' in response")
            else:
                print(f"✗ Failed to get granules (status: {granules_resp.status_code if granules_resp else 'None'})")
        else:
            print("✗ No granules link in summary")
    else:
        print(f"✗ Failed to fetch package (status: {resp.status_code if resp else 'None'})")
    
    print("\n" + "=" * 60)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Floor speech collector using GovInfo CREC API")
    parser.add_argument("--start-date", type=str, default="2024-01-15", 
                        help="Start date (YYYY-MM-DD, default: 2024-01-15)")
    parser.add_argument("--end-date", type=str, default="2024-06-30",
                        help="End date (YYYY-MM-DD, default: 2024-06-30)")
    parser.add_argument("--max-per-person", type=int, default=30, 
                        help="Max speeches per person (default: 30)")
    parser.add_argument("--max-days", type=int, default=30,
                        help="Max days of Congressional Record to process (default: 30)")
    parser.add_argument("--output", type=str, default="../../data/config/floor_speeches_congress_api.json", 
                        help="Output JSON file")
    parser.add_argument("--overlap-check", type=str, default="../../data/config/collected_speeches.json",
                        help="Path to collected_speeches.json for overlap check")
    parser.add_argument("--test", action="store_true", help="Run detailed API test")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--fresh-start", action="store_true", 
                        help="Start fresh (ignore existing output file)")
    args = parser.parse_args()
    
    # Run test mode
    if args.test:
        test_api_detailed()
        return
    
    CONFIG["max_speeches_per_person"] = args.max_per_person
    CONFIG["date_start"] = args.start_date
    CONFIG["date_end"] = args.end_date
    
    print("=" * 60)
    print("Floor Speech Collector - GovInfo CREC API")
    print("=" * 60)
    print(f"\nDate range: {CONFIG['date_start']} to {CONFIG['date_end']}")
    print(f"Max {args.max_per_person} speeches per person")
    print(f"Processing up to {args.max_days} days of Congressional Record")
    
    # Load existing people for overlap check (lightweight bonus)
    print("\n[1/5] Loading existing people for overlap check...")
    existing_people = load_existing_people(args.overlap_check)
    if existing_people:
        print(f"  Found {len(existing_people)} existing people to prioritize")
    else:
        print("  No existing people found - will collect all speeches")
    
    # Build member lookup for matching CREC speakers
    print("\n[2/5] Building member name lookup table...")
    member_lookup = build_member_lookup(existing_people)
    print(f"  Created {len(member_lookup)} name variations to match")
    
    # Output file setup
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing output to avoid duplicates (unless fresh start requested)
    existing_speeches_count = 0
    if output_path.exists() and not args.fresh_start:
        try:
            output = json.loads(output_path.read_text(encoding="utf-8"))
            existing_speeches_count = sum(
                len(v) for p in output.values() 
                for k, v in p.items() if k.endswith("floor_speeches")
            )
            print(f"  Loaded existing output: {len(output)} people, {existing_speeches_count} speeches")
            print(f"  ✓ Will merge new speeches with existing data")
        except:
            output = {}
            print(f"  Could not load existing file, starting fresh")
    else:
        output = {}
        if args.fresh_start:
            print(f"  Fresh start requested - ignoring existing file")
    
    # Get CREC packages (daily issues)
    print("\n[3/5] Fetching Congressional Record packages...")
    packages = get_crec_packages(CONFIG["date_start"], CONFIG["date_end"], max_results=args.max_days * 2)
    
    if not packages:
        print("  ✗ No packages found!")
        return
    
    # Limit packages to process
    packages = packages[:args.max_days]
    print(f"  Processing {len(packages)} daily issues...")
    
    # Track speech counts per person
    speech_counts = {}
    for person_key, data in output.items():
        count = sum(len(v) for k, v in data.items() if k.endswith("floor_speeches"))
        speech_counts[person_key] = count
    
    # Process each package
    print("\n[4/5] Extracting floor speeches from Congressional Record...")
    print(f"  Limit: {args.max_per_person} speeches per person")
    total_new = 0
    debug = args.debug
    people_at_limit = 0
    
    for idx, package in enumerate(tqdm(packages, desc="Processing daily issues")):
        package_id = package["packageId"]
        date = package["dateIssued"]
        
        if debug and idx < 3:
            print(f"\n  DEBUG: Processing {package_id} ({date})")
        
        try:
            # Extract speeches from this daily issue
            speeches_by_person = process_crec_package(
                package_id, 
                member_lookup, 
                speech_counts, 
                args.max_per_person,
                debug=(debug and idx < 3)
            )
            
            if debug and idx < 3:
                print(f"  DEBUG: Found {len(speeches_by_person)} people with speeches")
                for person, spch_list in list(speeches_by_person.items())[:3]:
                    print(f"    - {person}: {len(spch_list)} speeches")
            
            # Add to output
            for person_key, speeches in speeches_by_person.items():
                # Initialize person if needed
                if person_key not in output:
                    output[person_key] = {}
                
                # Determine categories
                for speech in speeches:
                    chamber = speech["chamber"]
                    category = f"{chamber}_floor_speeches"
                    
                    if category not in output[person_key]:
                        output[person_key][category] = {}
                    
                    # Create filename
                    date_str = speech["date"].replace("-", "_")
                    granule_id = speech["granuleId"].replace("-", "_")
                    filename = f"floor_{date_str}_{granule_id}.txt"
                    
                    # Add if not duplicate
                    if filename not in output[person_key][category]:
                        output[person_key][category][filename] = speech["url"]
                        total_new += 1
                        speech_counts[person_key] = speech_counts.get(person_key, 0) + 1
            
            # Check if we can stop early (all target people at limit)
            people_at_limit = sum(1 for k, v in speech_counts.items() if v >= args.max_per_person and k in existing_people)
            if people_at_limit >= len(existing_people) and len(existing_people) > 0:
                print(f"\n  All {len(existing_people)} target people reached limit! Stopping early.")
                break
        
        except Exception as e:
            print(f"\n  Error processing {package_id}: {e}")
            traceback.print_exc()
            continue
        
        # Save checkpoint every 10 packages
        if (idx + 1) % 10 == 0:
            output_path.write_text(json.dumps(output, indent=4), encoding="utf-8")
            if not debug:
                print(f"\n  Checkpoint: Saved progress at {idx + 1}/{len(packages)} packages")
    
    # Final save
    print("\n[5/5] Saving results...")
    output_path.write_text(json.dumps(output, indent=4), encoding="utf-8")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_people = len([p for p in output.values() if any(k.endswith("floor_speeches") for k in p.keys())])
    total_speeches_now = sum(
        len(v) for p in output.values() 
        for k, v in p.items() if k.endswith("floor_speeches")
    )
    people_at_limit_final = sum(1 for k, v in speech_counts.items() if v >= args.max_per_person)
    
    print(f"\n Collection Statistics:")
    print(f"  People with floor speeches: {total_people}")
    print(f"  Speeches before this run: {existing_speeches_count}")
    print(f"  New speeches this run: {total_new}")
    print(f"  Total speeches now: {total_speeches_now}")
    print(f"  People at limit ({args.max_per_person}): {people_at_limit_final}/{total_people}")
    
    print(f"\n✓ Saved to: {output_path}")
    
    # Show top speakers
    if speech_counts:
        print(f"\n  Top 10 Speakers (by total count):")
        sorted_speakers = sorted(speech_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (person, count) in enumerate(sorted_speakers, 1):
            status = "✓ AT LIMIT" if count >= args.max_per_person else f"({args.max_per_person - count} more)"
            print(f"  {i:2}. {person:30s} {count:3d} speeches {status}")
    
    # Show people who need more speeches
    people_need_more = [(k, v) for k, v in speech_counts.items() if v < args.max_per_person and k in existing_people]
    if people_need_more and len(people_need_more) <= 20:
        print(f"\n Priority people still needing speeches ({len(people_need_more)}):")
        people_need_more.sort(key=lambda x: x[1])
        for person, count in people_need_more[:10]:
            needed = args.max_per_person - count
            print(f"  • {person:30s} has {count}, needs {needed} more")
        if len(people_need_more) > 10:
            print(f"  ... and {len(people_need_more) - 10} more")
    
    print(f"\n Tips for next run:")
    print(f"  Use different --start-date and --end-date to collect from other periods")
    print(f"  Example: --start-date 2023-01-01 --end-date 2023-12-31")
    print(f"  Script automatically merges with existing data (no duplicates)")
    print(f"  Use --fresh-start to ignore existing file and start over")
    print(f"  Adjust --max-per-person to balance collection speed vs coverage")
    
    print(f"\n Note: Using GovInfo Congressional Record (CREC)")
    print(f"         Actual verbatim floor remarks from official record")
    
    return output

if __name__ == "__main__":
    main()
