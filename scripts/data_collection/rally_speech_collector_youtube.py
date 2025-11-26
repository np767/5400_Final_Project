"""
Rally Speech Collector - YouTube Auto-Captions

How it works:
1. Loads all speakers from collected_speeches.json and floor_speeches_congress_api.json
   (merges and deduplicates for complete coverage)
2. For each speaker, automatically searches YouTube with queries like:
   - "{speaker name} victory speech 2024"
   - "{speaker name} campaign rally 2020"
   - "{speaker name} convention speech 2024"
3. Downloads YouTube's auto-generated captions for each video
4. Saves transcripts to rally_speeches_youtube.json

Requirements:
    pip install youtube-transcript-api youtube-search-python

Usage:
    python rally_speech_collector_youtube.py --max-per-person 5
    python rally_speech_collector_youtube.py --test  # Test with 2 speakers first
    python rally_speech_collector_youtube.py --speaker bernie_sanders  # Single speaker
"""

import json
import argparse
import re
from pathlib import Path
from datetime import datetime
import time
from typing import Dict, List, Optional
   
try:
    from youtubesearchpython import VideosSearch
    from youtube_transcript_api import YouTubeTranscriptApi
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install youtube-transcript-api youtube-search-python")


CONFIG = {
    "collected_speeches_path": "../../data/config/collected_speeches.json",
    "floor_speeches_path": "../../data/config/floor_speeches_congress_api.json",
    "output_json": "../../data/config/rally_speeches_youtube.json",
    "min_video_duration_minutes": 5,    # Skip videos shorter than 5 mins (likely not speeches)
    "search_years": [2016, 2018, 2020, 2022, 2024, 2025],  # Campaign years
}


def slugify(text: str) -> str:
    """Convert text to URL-safe slug"""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text


def load_speaker_roster() -> List[Dict]:
    """
    Load existing speakers from both collected_speeches.json and floor_speeches_congress_api.json
    Merges and deduplicates by person_key for complete coverage
    """
    speakers_dict = {}  # Use dict to auto-deduplicate by key
    
    # Load from collected_speeches.json
    path1 = Path(__file__).parent / CONFIG["collected_speeches_path"]
    if path1.exists():
        data = json.loads(path1.read_text(encoding="utf-8"))
        for person_key, person_data in data.items():
            full_name = person_data.get("name", person_key.replace("_", " ").title())
            speakers_dict[person_key] = {
                "key": person_key,
                "name": full_name,
                "party": person_data.get("party", "Unknown"),
            }
        print(f"  Loaded {len(data)} speakers from collected_speeches.json")
    
    # Load from floor_speeches_congress_api.json
    path2 = Path(__file__).parent / CONFIG["floor_speeches_path"]
    if path2.exists():
        data = json.loads(path2.read_text(encoding="utf-8"))
        new_speakers = 0
        for person_key, person_data in data.items():
            if person_key not in speakers_dict:  # Only add if not already present
                full_name = person_data.get("name", person_key.replace("_", " ").title())
                speakers_dict[person_key] = {
                    "key": person_key,
                    "name": full_name,
                    "party": person_data.get("party", "Unknown"),
                }
                new_speakers += 1
        print(f"  Added {new_speakers} additional speakers from floor_speeches_congress_api.json")
    
    if not speakers_dict:
        print(f"Error: No speaker data found")
        return []
    
    return list(speakers_dict.values())


def build_search_queries(speaker: Dict, max_per_type: int = 2) -> List[Dict]:
    """Build YouTube search queries for a speaker"""
    name = speaker["name"]
    queries = []
    
    # Victory speeches
    for year in CONFIG["search_years"]:
        queries.append({
            "query": f"{name} victory speech {year}",
            "type": "victory_speech",
            "year": year
        })
    
    # Campaign rallies
    for year in CONFIG["search_years"]:
        queries.append({
            "query": f"{name} campaign rally {year}",
            "type": "campaign_rally",
            "year": year
        })
        queries.append({
            "query": f"{name} {speaker['party']} rally {year}",
            "type": "campaign_rally",
            "year": year
        })
    
    # Convention speeches
    for year in CONFIG["search_years"]:
        queries.append({
            "query": f"{name} convention speech {year}",
            "type": "convention_speech",
            "year": year
        })
    
    return queries


def search_youtube(query: str, max_results: int = 5) -> List[Dict]:
    """Search YouTube and return video metadata"""
    try:
        videos_search = VideosSearch(query, limit=max_results)
        results = videos_search.result()
        
        videos = []
        for video in results.get("result", []):
            duration_str = video.get("duration", "")
            video_id = video.get("id", "")
            
            # Parse duration (format: "MM:SS" or "HH:MM:SS")
            try:
                parts = duration_str.split(":")
                if len(parts) == 2:
                    duration_mins = int(parts[0])
                elif len(parts) == 3:
                    duration_mins = int(parts[0]) * 60 + int(parts[1])
                else:
                    duration_mins = 0
            except:
                duration_mins = 0
            
            # Filter by duration (skip very short videos)
            if duration_mins < CONFIG["min_video_duration_minutes"]:
                continue
            
            videos.append({
                "video_id": video_id,
                "title": video.get("title", ""),
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "duration_minutes": duration_mins,
                "channel": video.get("channel", {}).get("name", ""),
                "publish_date": video.get("publishedTime", ""),
            })
        
        return videos
    except Exception as e:
        print(f"  Search error: {e}")
        return []


def get_youtube_captions(video_id: str) -> Optional[str]:
    """Get existing YouTube captions/transcript"""
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
        
        # Combine all text segments
        full_text = " ".join([entry["text"] for entry in transcript_list])
        return full_text
    except Exception as e:
        print(f"    No captions available: {e}")
        return None




def collect_speeches_for_speaker(
    speaker: Dict, 
    max_speeches: int = 5,
    debug: bool = False
) -> Dict[str, List[Dict]]:
    """
    Automatically search YouTube and collect rally speeches for one speaker.
    
    The script searches YouTube with queries like:
    - "{name} victory speech 2024"
    - "{name} campaign rally 2020"
    - "{name} convention speech 2024"
    
    No manual searching required!
    """
    
    print(f"\n{'='*60}")
    print(f"Collecting speeches for: {speaker['name']}")
    print(f"{'='*60}")
    
    queries = build_search_queries(speaker, max_per_type=2)
    collected = {
        "partisan_rally_speeches": []
    }
    
    speeches_found = 0
    
    for query_info in queries:
        if speeches_found >= max_speeches:
            break
        
        query = query_info["query"]
        print(f"\n  Searching: {query}")
        
        videos = search_youtube(query, max_results=3)
        print(f"    Found {len(videos)} candidate videos")
        
        for video in videos:
            if speeches_found >= max_speeches:
                break
            
            print(f"    Processing: {video['title'][:60]}...")
            
            # Get YouTube auto-generated captions
            transcript = get_youtube_captions(video["video_id"])
            
            if transcript and len(transcript) > 500:  # Minimum length check
                speech_data = {
                    "title": video["title"],
                    "url": video["url"],
                    "date": query_info["year"],  # Approximate from search
                    "source": "youtube",
                    "method": "auto_captions",
                    "duration_minutes": video["duration_minutes"],
                    "transcript": transcript,
                    "video_id": video["video_id"],
                }
                
                collected["partisan_rally_speeches"].append(speech_data)
                speeches_found += 1
                print(f"    ✓ Collected speech #{speeches_found}")
            else:
                print(f"    ✗ No captions available or too short")
            
            time.sleep(1)  # Rate limiting
    
    print(f"\n  Total speeches collected: {speeches_found}")
    return collected


def main():
    parser = argparse.ArgumentParser(
        description="Automatically collect rally speeches from YouTube (no manual searching required!)"
    )
    parser.add_argument("--max-per-person", type=int, default=3,
                        help="Maximum speeches per person")
    parser.add_argument("--max-speakers", type=int, default=None,
                        help="Limit number of speakers to process")
    parser.add_argument("--speaker", type=str, default=None,
                        help="Process only this speaker (by key, e.g., 'bernie_sanders')")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: only process 2 speakers")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON file")
    
    args = parser.parse_args()
    
    print("="*60)
    print("Rally Speech Collector - YouTube Auto-Captions")
    print("="*60)
    print(f"Max speeches per person: {args.max_per_person}")
    print("The script will automatically search YouTube for each speaker!")
    
    # Load speakers from both sources
    print("\n[1/3] Loading speaker roster (merging from both JSONs)...")
    speakers = load_speaker_roster()
    print(f"  Total unique speakers: {len(speakers)}")
    
    # Filter speakers if requested
    if args.speaker:
        speakers = [s for s in speakers if s["key"] == args.speaker]
        print(f"  Filtered to 1 speaker: {args.speaker}")
    
    if args.test:
        speakers = speakers[:2]
        print(f"  Test mode: processing {len(speakers)} speakers")
    
    if args.max_speakers:
        speakers = speakers[:args.max_speakers]
        print(f"  Limited to {len(speakers)} speakers")
    
    # Collect speeches
    print("\n[2/3] Collecting speeches from YouTube...")
    output = {}
    
    for i, speaker in enumerate(speakers, 1):
        print(f"\n[Speaker {i}/{len(speakers)}]")
        
        speeches = collect_speeches_for_speaker(
            speaker,
            max_speeches=args.max_per_person
        )
        
        if speeches["partisan_rally_speeches"]:
            output[speaker["key"]] = speeches
        
        time.sleep(2)  # Rate limiting between speakers
    
    # Save output
    print("\n[3/3] Saving results...")
    output_path = Path(__file__).parent / (args.output or CONFIG["output_json"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    
    print(f"  ✓ Saved to {output_path}")
    
    # Summary
    total_speeches = sum(len(data["partisan_rally_speeches"]) for data in output.values())
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Speakers processed: {len(output)}")
    print(f"Total speeches collected: {total_speeches}")
    print(f"Average per speaker: {total_speeches / len(output) if output else 0:.1f}")
    print("="*60)


if __name__ == "__main__":
    main()

