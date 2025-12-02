"""
Rally Speech Collector - Audio Download + Speech-to-Text Transcription

Alternative approach to YouTube captions:
1. Search YouTube using official API (fast, reliable)
2. Download audio using yt-dlp (bypasses rate limits)
3. Transcribe using speech-to-text API (Google Cloud, AWS, or OpenAI Whisper)

This avoids YouTube's transcript API rate limits entirely.

Requirements:
    pip install yt-dlp google-cloud-speech openai-whisper
    # OR for free option:
    pip install yt-dlp openai-whisper  # Uses local Whisper model

Usage:
    python rally_speech_collector_audio_transcribe.py --max-per-person 5
    python rally_speech_collector_audio_transcribe.py --transcription-method whisper  # Free, local
    python rally_speech_collector_audio_transcribe.py --transcription-method google  # Requires Google Cloud setup
"""

import json
import argparse
import re
from pathlib import Path
from datetime import datetime
import time
import random
import subprocess
import tempfile
import os
import warnings
from typing import Dict, List, Optional

# Fix OpenMP library conflict on macOS
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")

# Suppress Whisper FP16 warning (expected on CPU)
warnings.filterwarnings("ignore", message="FP16 is not supported on CPU")

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    YOUTUBE_API_AVAILABLE = True
except ImportError:
    YOUTUBE_API_AVAILABLE = False
    HttpError = None
    print("Warning: google-api-python-client not installed")

try:
    from youtubesearchpython import VideosSearch
    YOUTUBE_SEARCH_AVAILABLE = True
except ImportError:
    YOUTUBE_SEARCH_AVAILABLE = False
    print("Warning: youtubesearchpython not installed (fallback unavailable)")

# Global flag to track if we've hit quota
QUOTA_EXCEEDED = False

# Load API keys from config file
def load_api_keys() -> Dict[str, str]:
    """Load API keys from api_keys.json file"""
    api_keys_path = Path(__file__).parent / "api_keys.json"
    if api_keys_path.exists():
        try:
            with open(api_keys_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load API keys from {api_keys_path}: {e}")
            return {}
    else:
        print(f"Warning: API keys file not found at {api_keys_path}")
        print(f"  Please create {api_keys_path} using api_keys.json.example as a template")
        return {}

API_KEYS = load_api_keys()

CONFIG = {
    "collected_speeches_path": "../../data/config/collected_speeches.json",
    "floor_speeches_path": "../../data/config/floor_speeches_congress_api.json",
    "output_json": "../../data/config/rally_speeches_audio_transcribed.json",
    "youtube_api_key": API_KEYS.get("youtube_api_key", ""),
    "min_video_duration_minutes": 3,  # Lowered from 5 to get more candidates
    "search_years": [2016, 2018, 2020, 2022, 2024, 2025],  # Added 2016, 2018
    "max_videos_per_query": 5,  # Reduced to save quota (was 10)
    "max_queries_per_speaker": 18,  # Reduced to save quota (was 24)
    "delay_between_downloads": 3,  # Reduced from 5 to speed up
    "delay_between_speakers": 5,  # Reduced from 10 to speed up
    "min_transcript_length": 300,  # Lowered from 500 to accept shorter speeches
    # Rate-limit-avoidance settings for gap-filling mode
    "gap_filling_mode": False,  # Set to True when filling gaps
    "gap_filling_delay_between_downloads": 10,  # Longer delay to avoid rate limits
    "gap_filling_delay_between_speakers": 15,  # Longer delay between speakers
    "gap_filling_delay_before_search": 2,  # Delay before each search
    "gap_filling_min_transcript_length": 150,  # Very lenient for 0-speech speakers
}


def search_youtube_unofficial(query: str, max_results: int = 5) -> List[Dict]:
    """Search YouTube using unofficial API (no quota)"""
    global QUOTA_EXCEEDED
    if not YOUTUBE_SEARCH_AVAILABLE:
        return []
    
    try:
        search = VideosSearch(query, limit=max_results)
        results = search.result()
        
        videos = []
        for item in results.get('result', [])[:max_results]:
            video_id = item.get('id', '')
            if not video_id:
                continue
            
            # Parse duration from string like "10:30" or "1:23:45"
            duration_str = item.get('duration', '')
            duration_mins = 0
            try:
                parts = duration_str.split(':')
                if len(parts) == 2:  # MM:SS
                    duration_mins = int(parts[0]) + int(parts[1]) / 60
                elif len(parts) == 3:  # HH:MM:SS
                    duration_mins = int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 60
            except:
                duration_mins = 0
            
            if duration_mins < CONFIG["min_video_duration_minutes"]:
                continue
            
            videos.append({
                "video_id": video_id,
                "title": item.get('title', ''),
                "url": item.get('link', f"https://www.youtube.com/watch?v={video_id}"),
                "duration_minutes": int(duration_mins),
                "channel": item.get('channel', {}).get('name', ''),
                "publish_date": item.get('publishedTime', ''),
            })
        
        return videos
    except Exception as e:
        return []


def search_youtube_official(query: str, max_results: int = 5) -> List[Dict]:
    """Search YouTube using official API (with quota)"""
    global QUOTA_EXCEEDED
    if not CONFIG["youtube_api_key"] or not YOUTUBE_API_AVAILABLE or QUOTA_EXCEEDED:
        return []
    
    try:
        youtube = build('youtube', 'v3', developerKey=CONFIG["youtube_api_key"])
        request = youtube.search().list(
            part='snippet',
            q=query,
            type='video',
            maxResults=max_results,
            order='relevance'
        )
        response = request.execute()
        
        videos = []
        for item in response.get('items', []):
            video_id = item['id']['videoId']
            
            # Get video details for duration
            video_request = youtube.videos().list(
                part='contentDetails,snippet',
                id=video_id
            )
            video_response = video_request.execute()
            
            if not video_response.get('items'):
                continue
            
            video_details = video_response['items'][0]
            duration_str = video_details['contentDetails']['duration']
            
            # Parse ISO 8601 duration
            duration_mins = 0
            try:
                match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
                if match:
                    hours = int(match.group(1) or 0)
                    minutes = int(match.group(2) or 0)
                    seconds = int(match.group(3) or 0)
                    duration_mins = hours * 60 + minutes + (seconds / 60)
            except:
                duration_mins = 0
            
            if duration_mins < CONFIG["min_video_duration_minutes"]:
                continue
            
            videos.append({
                "video_id": video_id,
                "title": video_details['snippet']['title'],
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "duration_minutes": int(duration_mins),
                "channel": video_details['snippet'].get('channelTitle', ''),
                "publish_date": video_details['snippet'].get('publishedAt', ''),
            })
        
        return videos
    except HttpError as e:
        if e.resp.status == 403 and 'quotaExceeded' in str(e):
            QUOTA_EXCEEDED = True
            print(f"  ⚠ YouTube API quota exceeded, switching to fallback search")
            return search_youtube_unofficial(query, max_results)
        print(f"  Search error: {e}")
        return []
    except Exception as e:
        print(f"  Search error: {e}")
        return []


def search_youtube(query: str, max_results: int = 5) -> List[Dict]:
    """Search YouTube - tries official API first, falls back to unofficial"""
    global QUOTA_EXCEEDED
    
    # Conservative delay before search to avoid rate limiting
    delay = CONFIG.get("gap_filling_delay_before_search", 0.5) if CONFIG.get("gap_filling_mode", False) else 0.5
    time.sleep(delay + random.uniform(0, 1))
    
    # If quota exceeded, use unofficial immediately
    if QUOTA_EXCEEDED:
        return search_youtube_unofficial(query, max_results)
    
    # Try official first
    videos = search_youtube_official(query, max_results)
    
    # If quota exceeded during search, fallback already handled
    # If no results and we haven't hit quota, try unofficial as backup
    if not videos and not QUOTA_EXCEEDED and YOUTUBE_SEARCH_AVAILABLE:
        return search_youtube_unofficial(query, max_results)
    
    return videos


def download_audio(video_id: str, output_path: Path, verbose: bool = False) -> Optional[str]:
    """Download audio from YouTube video using yt-dlp"""
    try:
        # Use yt-dlp to download audio only
        # Try mp3 first (smaller, faster), fallback to best available
        # Use --extractor-args to avoid JavaScript requirement
        cmd = [
            "yt-dlp",
            "-x",  # Extract audio
            "--audio-format", "mp3",  # Use mp3 (smaller than wav, still good quality)
            "--audio-quality", "5",  # Good quality (0=best, 9=worst)
            "--extractor-args", "youtube:player_client=default",  # Avoid JS requirement
            "-o", str(output_path / f"{video_id}.%(ext)s"),
            f"https://www.youtube.com/watch?v={video_id}",
        ]
        
        if not verbose:
            cmd.append("--quiet")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            # Find the downloaded file (could be mp3, m4a, opus, etc.)
            for ext in ["mp3", "m4a", "opus", "wav", "webm"]:
                audio_file = output_path / f"{video_id}.{ext}"
                if audio_file.exists():
                    return str(audio_file)
        else:
            # Try without extractor args as fallback
            if "JavaScript runtime" in result.stderr or "player_client" in result.stderr:
                cmd_fallback = [
                    "yt-dlp",
                    "-x",
                    "--audio-format", "mp3",
                    "--audio-quality", "5",
                    "-o", str(output_path / f"{video_id}.%(ext)s"),
                    f"https://www.youtube.com/watch?v={video_id}",
                ]
                if not verbose:
                    cmd_fallback.append("--quiet")
                
                result_fallback = subprocess.run(cmd_fallback, capture_output=True, text=True, timeout=600)
                if result_fallback.returncode == 0:
                    for ext in ["mp3", "m4a", "opus", "wav", "webm"]:
                        audio_file = output_path / f"{video_id}.{ext}"
                        if audio_file.exists():
                            return str(audio_file)
            
            if verbose:
                error_msg = result.stderr[:300] if result.stderr else result.stdout[:300]
                print(f"    Download failed: {error_msg}")
        
        return None
    except subprocess.TimeoutExpired:
        if verbose:
            print(f"    Download timeout (video too long?)")
        return None
    except Exception as e:
        if verbose:
            print(f"    Download error: {e}")
        return None


def transcribe_whisper(audio_file: str) -> Optional[str]:
    """Transcribe using OpenAI Whisper (free, local)"""
    try:
        import whisper
        
        # Suppress warnings during transcription
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="FP16 is not supported on CPU")
            model = whisper.load_model("base")  # Options: tiny, base, small, medium, large
            result = model.transcribe(audio_file)
        
        return result["text"]
    except ImportError:
        print("    Error: openai-whisper not installed. Install with: pip install openai-whisper")
        return None
    except Exception as e:
        print(f"    Transcription error: {e}")
        return None


def transcribe_google_cloud(audio_file: str) -> Optional[str]:
    """Transcribe using Google Cloud Speech-to-Text API"""
    try:
        from google.cloud import speech
        
        client = speech.SpeechClient()
        
        with open(audio_file, "rb") as audio:
            content = audio.read()
        
        audio_config = speech.RecognitionAudio(content=content)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=16000,
            language_code="en-US",
        )
        
        response = client.recognize(config=config, audio=audio_config)
        
        transcript = " ".join([result.alternatives[0].transcript for result in response.results])
        return transcript
    except ImportError:
        print("    Error: google-cloud-speech not installed")
        return None
    except Exception as e:
        print(f"    Transcription error: {e}")
        return None


def collect_speeches_for_speaker(
    speaker: Dict,
    max_speeches: int = 5,
    transcription_method: str = "whisper",
    verbose: bool = False,
    existing_count: int = 0
) -> Dict:
    """Collect speeches by downloading audio and transcribing"""
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Collecting speeches for: {speaker['name']}")
        print(f"{'='*60}")
    
    queries = []
    # More diverse search queries
    query_templates = [
        "{name} victory speech {year}",
        "{name} campaign rally {year}",
        "{name} convention speech {year}",
        "{name} rally {year}",
        "{name} speech {year}",
        "{name} address {year}",
        "{name} remarks {year}",
        "{name} campaign speech {year}",
        "{name} election speech {year}",
        "{name} political rally {year}",
    ]
    
    # For speakers with 0 speeches, use more aggressive search strategies
    if existing_count == 0:
        # Extract first and last name for alternative formats
        name_parts = speaker['name'].split()
        # Handle multi-part names better
        # For "Blunt Rochester Lisa" -> first="Lisa", last="Blunt Rochester"
        # For "Lisa Blunt Rochester" -> first="Lisa", last="Blunt Rochester"
        if len(name_parts) >= 3:
            # If name has 3+ parts, assume last part is first name if it's a common first name pattern
            # Otherwise, assume first part is first name
            # Common pattern: "Last Middle First" (from key format) vs "First Middle Last" (normal)
            # Try both interpretations
            first_name = name_parts[-1]  # Last word might be first name
            last_name = " ".join(name_parts[:-1])  # Everything before last word
            # Also create reverse version
            first_name_reverse = name_parts[0]  # First word might be first name
            last_name_reverse = " ".join(name_parts[1:])  # Everything after first word
        elif len(name_parts) == 2:
            first_name = name_parts[0]
            last_name = name_parts[1]
            first_name_reverse = name_parts[1]
            last_name_reverse = name_parts[0]
        else:
            first_name = name_parts[0] if name_parts else speaker['name']
            last_name = name_parts[0] if name_parts else speaker['name']
            first_name_reverse = first_name
            last_name_reverse = last_name
        
        # Create properly ordered name (for keys like "blunt_rochester_lisa" -> "Lisa Blunt Rochester")
        # If name has 3+ parts, assume format is "Last Middle First" and reorder to "First Middle Last"
        if len(name_parts) >= 3:
            # Move last part to front: ["Blunt", "Rochester", "Lisa"] -> ["Lisa", "Blunt", "Rochester"]
            reversed_name = f"{name_parts[-1]} {' '.join(name_parts[:-1])}"
        elif len(name_parts) == 2:
            # For 2 parts, try both orders
            reversed_name = f"{name_parts[1]} {name_parts[0]}"
        else:
            reversed_name = speaker['name']
        
        # Add generic queries without year (broader search)
        generic_templates = [
            "{name} rally",
            "{reversed_name} rally",  # Try reversed name format
            "{name} campaign speech",
            "{reversed_name} campaign speech",
            "{name} victory speech",
            "{reversed_name} victory speech",
            "{name} political speech",
            "{reversed_name} political speech",
            "{name} election speech",
            "{reversed_name} election speech",
            "{name} campaign rally",
            "{reversed_name} campaign rally",
            "{name} speech",
            "{reversed_name} speech",
            "{name} address",
            "{reversed_name} address",
            "{name} remarks",
            "{reversed_name} remarks",
            # Try with titles
            "Senator {name} rally",
            "Senator {name} speech",
            "Congressman {name} rally",
            "Congressman {name} speech",
            "Congresswoman {name} rally",
            "Congresswoman {name} speech",
            "Rep {name} rally",
            "Rep {name} speech",
            "Sen {name} rally",
            "Sen {name} speech",
            # Try with just last name
            "{last_name} rally",
            "{last_name} speech",
            "{last_name} campaign speech",
            # Try with first name + last name variations (normal order)
            "{first_name} {last_name} rally",
            "{first_name} {last_name} speech",
            # Try reverse order (in case name was stored backwards)
            "{first_name_reverse} {last_name_reverse} rally",
            "{first_name_reverse} {last_name_reverse} speech",
            # Try with just the reversed last name
            "{last_name_reverse} rally",
            "{last_name_reverse} speech",
        ]
        
        for template in generic_templates:
            try:
                query = template.format(
                    name=speaker['name'],
                    reversed_name=reversed_name,
                    first_name=first_name,
                    last_name=last_name,
                    first_name_reverse=first_name_reverse if len(name_parts) >= 2 else first_name,
                    last_name_reverse=last_name_reverse if len(name_parts) >= 2 else last_name
                )
                queries.append({
                    "query": query,
                    "year": None  # Will use video publish date if available
                })
            except KeyError:
                # Skip templates that don't match format
                pass
    
    for year in CONFIG["search_years"]:
        for template in query_templates:
            queries.append({
                "query": template.format(name=speaker['name'], year=year),
                "year": year
            })
    
    queries = queries[:CONFIG["max_queries_per_speaker"]]
    collected = {"partisan_rally_speeches": []}
    speeches_found = 0
    errors = {
        "no_videos": 0,
        "download_failed": 0,
        "transcription_failed": 0,
        "too_short": 0,
    }
    
    # Create temp directory for audio files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        for query_info in queries:
            if speeches_found >= max_speeches:
                break
            
            query = query_info["query"]
            if verbose:
                print(f"\n  Searching: {query}")
            
            videos = search_youtube(query, max_results=CONFIG["max_videos_per_query"])
            if verbose:
                print(f"    Found {len(videos)} candidate videos")
            
            if not videos:
                errors["no_videos"] += 1
                continue
            
            for video in videos:
                if speeches_found >= max_speeches:
                    break
                
                if verbose:
                    print(f"    Processing: {video['title'][:60]}...")
                
                # Download audio
                audio_file = download_audio(video["video_id"], temp_path, verbose=verbose)
                if not audio_file:
                    errors["download_failed"] += 1
                    if verbose:
                        print(f"    ✗ Failed to download audio")
                    continue
                
                if verbose:
                    print(f"    ✓ Downloaded audio: {Path(audio_file).name}")
                
                # Use longer delay in gap-filling mode to avoid rate limits
                delay = CONFIG.get("gap_filling_delay_between_downloads", CONFIG["delay_between_downloads"]) if CONFIG.get("gap_filling_mode", False) else CONFIG["delay_between_downloads"]
                time.sleep(delay + random.uniform(0, 2))  # Add random jitter
                
                # Transcribe
                if verbose:
                    print(f"    Transcribing with {transcription_method}...")
                
                if transcription_method == "whisper":
                    transcript = transcribe_whisper(audio_file)
                elif transcription_method == "google":
                    transcript = transcribe_google_cloud(audio_file)
                else:
                    transcript = None
                
                # Clean up audio file
                try:
                    os.remove(audio_file)
                except:
                    pass
                
                if not transcript:
                    errors["transcription_failed"] += 1
                    if verbose:
                        print(f"    ✗ Transcription failed")
                    continue
                
                # For speakers with 0 speeches, be more lenient with transcript length
                if existing_count == 0 and CONFIG.get("gap_filling_mode", False):
                    min_length = CONFIG.get("gap_filling_min_transcript_length", 150)
                elif existing_count == 0:
                    min_length = max(200, CONFIG["min_transcript_length"] // 2)
                else:
                    min_length = CONFIG["min_transcript_length"]
                
                if len(transcript) < min_length:
                    errors["too_short"] += 1
                    if verbose:
                        print(f"    ✗ Transcript too short ({len(transcript)} chars, need {min_length})")
                    continue
                
                # Success! Use query year or try to extract from video publish date
                speech_year = query_info["year"]
                if speech_year is None and video.get("publish_date"):
                    try:
                        # Try to extract year from publish date (format: "2020-01-01T00:00:00Z")
                        speech_year = int(video["publish_date"][:4])
                    except:
                        speech_year = None
                
                # Success!
                speech_data = {
                    "title": video["title"],
                    "url": video["url"],
                    "date": speech_year,
                    "source": "youtube_audio",
                    "method": f"audio_download_{transcription_method}",
                    "duration_minutes": video["duration_minutes"],
                    "transcript": transcript,
                    "video_id": video["video_id"],
                }
                
                collected["partisan_rally_speeches"].append(speech_data)
                speeches_found += 1
                if verbose:
                    print(f"    ✓ Collected speech #{speeches_found} ({len(transcript)} chars)")
    
    if verbose and speeches_found < max_speeches:
        print(f"\n  Summary: Found {speeches_found}/{max_speeches} speeches")
        print(f"    Errors: {errors['no_videos']} no videos, {errors['download_failed']} download failed, "
              f"{errors['transcription_failed']} transcription failed, {errors['too_short']} too short")
    
    return {"partisan_rally_speeches": collected["partisan_rally_speeches"]}


def load_speaker_roster() -> List[Dict]:
    """Load speakers from both JSON files"""
    speakers_dict = {}
    
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
            if person_key not in speakers_dict:
                full_name = person_data.get("name", person_key.replace("_", " ").title())
                speakers_dict[person_key] = {
                    "key": person_key,
                    "name": full_name,
                    "party": person_data.get("party", "Unknown"),
                }
                new_speakers += 1
        print(f"  Added {new_speakers} additional speakers from floor_speeches_congress_api.json")
    
    return list(speakers_dict.values())


def main():
    parser = argparse.ArgumentParser(description="Collect rally speeches via audio download + transcription")
    parser.add_argument("--max-per-person", type=int, default=3, help="Max speeches per person")
    parser.add_argument("--transcription-method", choices=["whisper", "google"], default="whisper",
                        help="Transcription method: whisper (free, local) or google (requires setup)")
    parser.add_argument("--test", action="store_true", help="Test with 2 speakers")
    parser.add_argument("--speaker", type=str, help="Process single speaker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")
    parser.add_argument("--save-interval", type=int, default=10, help="Save progress every N speakers (default: 10)")
    parser.add_argument("--zero-only", action="store_true", help="Only process speakers with 0 speeches (gap-filling mode)")
    
    args = parser.parse_args()
    
    # Enable gap-filling mode if requested
    if args.zero_only:
        CONFIG["gap_filling_mode"] = True
        print("  ⚠ Gap-filling mode enabled: Using longer delays to avoid rate limits")
    
    start_time = datetime.now()
    print("="*60)
    print("Rally Speech Collector - Audio Download + Transcription")
    print("="*60)
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Transcription method: {args.transcription_method}")
    print(f"Max speeches per person: {args.max_per_person}")
    print("\nThis script:")
    print("1. Searches YouTube using official API (fast)")
    print("2. Downloads audio using yt-dlp (bypasses rate limits)")
    print("3. Transcribes using speech-to-text (no YouTube API needed)")
    
    # Check dependencies
    try:
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
        print("  ✓ yt-dlp installed")
    except:
        print("  ✗ yt-dlp not found. Install with: pip install yt-dlp")
        return
    
    # Check for ffmpeg (needed for audio extraction)
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        print("  ✓ ffmpeg installed")
    except:
        print("  ⚠ ffmpeg not found - audio extraction may fail")
        print("    Install with: brew install ffmpeg (macOS) or apt-get install ffmpeg (Linux)")
    
    if args.transcription_method == "whisper":
        try:
            import whisper
            print("  ✓ openai-whisper installed")
        except ImportError:
            print("  ✗ openai-whisper not found. Install with: pip install openai-whisper")
            return
    
    # Check for search APIs
    if YOUTUBE_API_AVAILABLE:
        print("  ✓ YouTube Data API available (official)")
    else:
        print("  ⚠ YouTube Data API not available")
    
    if YOUTUBE_SEARCH_AVAILABLE:
        print("  ✓ youtubesearchpython available (fallback, no quota)")
    else:
        print("  ⚠ youtubesearchpython not installed - install with: pip install youtubesearchpython")
        print("    (Fallback will not work if quota is exceeded)")
    
    # Load speakers
    print("\n[1/3] Loading speaker roster...")
    speakers = load_speaker_roster()
    print(f"  Total unique speakers: {len(speakers)}")
    
    # Filter speakers
    if args.speaker:
        speakers = [s for s in speakers if s["key"] == args.speaker]
        print(f"  Filtered to 1 speaker: {args.speaker}")
    
    if args.test:
        speakers = speakers[:2]
        print(f"  Test mode: processing {len(speakers)} speakers")
    
    # Load existing data
    output_path = Path(__file__).parent / (args.output or CONFIG["output_json"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists():
        try:
            output = json.loads(output_path.read_text(encoding="utf-8"))
            existing_speakers = len(output)
            existing_speeches = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
            print(f"  Loaded existing data: {existing_speakers} speakers, {existing_speeches} speeches")
            
            # Skip speakers who already have enough speeches (but still process those with 0)
            speakers_to_process = []
            for speaker in speakers:
                existing_count = len(output.get(speaker["key"], {}).get("partisan_rally_speeches", []))
                # In zero-only mode, only process speakers with exactly 0 speeches
                if args.zero_only:
                    if existing_count == 0:
                        speakers_to_process.append(speaker)
                    elif args.verbose:
                        print(f"  Skipping {speaker['name']} - has {existing_count} speeches (zero-only mode)")
                # Normal mode: process if below max
                elif existing_count < args.max_per_person:
                    speakers_to_process.append(speaker)
                else:
                    if args.verbose:
                        print(f"  Skipping {speaker['name']} - already has {existing_count} speeches")
            
            if len(speakers_to_process) < len(speakers):
                skipped = len(speakers) - len(speakers_to_process)
                mode_str = "zero-only mode" if args.zero_only else "already have enough speeches"
                print(f"  Skipping {skipped} speakers ({mode_str})")
                speakers = speakers_to_process
        except Exception as e:
            print(f"  Warning: Could not load existing data ({e}), starting fresh")
            output = {}
    else:
        output = {}
    
    # Collect speeches
    print("\n[2/3] Collecting speeches...")
    for i, speaker in enumerate(speakers, 1):
        progress_pct = (i / len(speakers)) * 100
        speaker_start = datetime.now()
        
        if args.verbose:
            print(f"\n[Speaker {i}/{len(speakers)}] - {speaker_start.strftime('%H:%M:%S')}")
        else:
            print(f"[{progress_pct:.1f}%] Processing {speaker['name']} ({i}/{len(speakers)})...", end=" ", flush=True)
        
        # Get existing count to pass to collection function
        existing_count = len(output.get(speaker["key"], {}).get("partisan_rally_speeches", []))
        
        result = collect_speeches_for_speaker(
            speaker,
            max_speeches=args.max_per_person,
            transcription_method=args.transcription_method,
            verbose=args.verbose,
            existing_count=existing_count
        )
        
        speeches = result.get("partisan_rally_speeches", [])
        
        # Always initialize speaker entry to preserve existing data
        if speaker["key"] not in output:
            output[speaker["key"]] = {"partisan_rally_speeches": []}
        
        if speeches:
            # Merge with existing (avoid duplicates by video_id)
            existing_video_ids = {s.get("video_id") for s in output[speaker["key"]].get("partisan_rally_speeches", [])}
            new_speeches = []
            for speech in speeches:
                video_id = speech.get("video_id")
                if video_id and video_id not in existing_video_ids:
                    output[speaker["key"]]["partisan_rally_speeches"].append(speech)
                    existing_video_ids.add(video_id)
                    new_speeches.append(speech)
            
            if not args.verbose:
                existing_count = len(output[speaker["key"]].get("partisan_rally_speeches", []))
                if new_speeches:
                    print(f"✓ Collected {len(new_speeches)} new speeches (Total: {existing_count})")
                else:
                    print(f"✓ No new speeches (Total: {existing_count})")
        else:
            if not args.verbose:
                existing_count = len(output[speaker["key"]].get("partisan_rally_speeches", []))
                print(f"✗ No speeches collected (Total: {existing_count})")
        
        # Save periodically
        if i % args.save_interval == 0 or i == len(speakers):
            output_path.write_text(
                json.dumps(output, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            if not args.verbose:
                total = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
                print(f"  [Saved] {total} speeches from {len(output)} speakers")
        
        # Use longer delay in gap-filling mode
        delay = CONFIG.get("gap_filling_delay_between_speakers", CONFIG["delay_between_speakers"]) if CONFIG.get("gap_filling_mode", False) else CONFIG["delay_between_speakers"]
        time.sleep(delay + random.uniform(0, 3))  # Add random jitter
    
    # Final save
    print("\n[3/3] Final save...")
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    print(f"  ✓ Saved to {output_path}")
    
    # Summary
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds() / 60
    total_speeches = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total time: {total_time:.1f} minutes ({total_time/60:.2f} hours)")
    speakers_processed = len(speakers)  # Number actually processed (after filtering)
    print(f"Speakers processed: {speakers_processed}/{speakers_processed}")
    print(f"Total speakers in output: {len(output)}")
    print(f"Total speeches collected: {total_speeches}")
    print(f"Average per speaker: {total_speeches / len(output) if output else 0:.1f}")
    if speakers_processed > 0:
        print(f"Average time per speaker: {total_time / speakers_processed:.1f} minutes")
    print("="*60)


if __name__ == "__main__":
    main()

