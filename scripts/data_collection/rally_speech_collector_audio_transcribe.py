"""
Rally Speech Collector - Audio Download + Speech-to-Text Transcription

1. Search YouTube using official API (fast, reliable)
2. Download audio using yt-dlp (bypasses rate limits)
3. Transcribe using speech-to-text API (Google Cloud, AWS, or OpenAI Whisper)

Avoids YouTube's transcript API rate limits.

Requirements:
    pip install yt-dlp google-cloud-speech openai-whisper
    # OR for free option:
    pip install yt-dlp openai-whisper  # Uses local Whisper model

Usage:
    python rally_speech_collector_audio_transcribe.py --max-per-person 5
    python rally_speech_collector_audio_transcribe.py --transcription-method whisper  # Free, local
    python rally_speech_collector_audio_transcribe.py --transcription-method google  # Requires Google Cloud setup

        Reach out to Vinny if you want to use my API key.
"""

import json
import argparse
import re
import logging
from pathlib import Path
from datetime import datetime
import time
import random
import subprocess
import tempfile
import warnings
from typing import Dict, List, Optional

# Fix OpenMP library conflict on macOS
import os
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")

# Suppress Whisper FP16 warning (expected on CPU)
warnings.filterwarnings("ignore", message="FP16 is not supported on CPU")

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    YOUTUBE_API_AVAILABLE = True
except ImportError:
    YOUTUBE_API_AVAILABLE = False

try:
    from youtubesearchpython import VideosSearch
    YOUTUBE_SEARCH_AVAILABLE = True
except ImportError:
    YOUTUBE_SEARCH_AVAILABLE = False

# Global flag to track if we've hit quota
QUOTA_EXCEEDED = False

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

# Load API keys from config file
def load_api_keys() -> Dict[str, str]:
    """Load API keys from api_keys.json file"""
    api_keys_path = Path(__file__).parent / "api_keys.json"
    if api_keys_path.exists():
        try:
            with open(api_keys_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not load API keys from {api_keys_path}: {e}")
            return {}
    else:
        logging.warning(f"API keys file not found at {api_keys_path}")
        logging.info(f"  Please create {api_keys_path} using api_keys.json.example as a template")
        return {}

API_KEYS = load_api_keys()

CONFIG = {
    "collected_speeches_path": "../../data/config/collected_speeches.json",
    "floor_speeches_path": "../../data/config/floor_speeches_congress_api.json",
    "output_json": "../../data/config/rally_speeches_audio_transcribed.json",
    "youtube_api_key": API_KEYS.get("youtube_api_key", ""),
    "min_video_duration_minutes": 3,
    "search_years": [2016, 2018, 2020, 2022, 2024, 2025],
    "max_videos_per_query": 5,
    "max_queries_per_speaker": 18,
    "delay_between_downloads": 3,
    "delay_between_speakers": 5,
    "min_transcript_length": 300,
    "gap_filling_mode": False,
    "gap_filling_delay_between_downloads": 10,
    "gap_filling_delay_between_speakers": 15,
    "gap_filling_delay_before_search": 2,
    "gap_filling_min_transcript_length": 150,
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
    except Exception as e:
        if hasattr(e, 'resp') and hasattr(e.resp, 'status') and e.resp.status == 403 and 'quotaExceeded' in str(e):
            QUOTA_EXCEEDED = True
            logging.warning("YouTube API quota exceeded, switching to fallback search")
            return search_youtube_unofficial(query, max_results)
        logging.error(f"Search error: {e}")
        return []


def search_youtube(query: str, max_results: int = 5) -> List[Dict]:
    """Search YouTube - tries official API first, falls back to unofficial"""
    global QUOTA_EXCEEDED
    
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


def download_audio(video_id: str, output_path: Path) -> Optional[str]:
    """Download audio from YouTube video using yt-dlp"""
    try:
        cmd = [
            "yt-dlp",
            "-x",
            "--audio-format", "mp3",
            "--audio-quality", "5",
            "--extractor-args", "youtube:player_client=default",
            "--quiet",
            "-o", str(output_path / f"{video_id}.%(ext)s"),
            f"https://www.youtube.com/watch?v={video_id}",
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            for ext in ["mp3", "m4a", "opus", "wav", "webm"]:
                audio_file = output_path / f"{video_id}.{ext}"
                if audio_file.exists():
                    return str(audio_file)
        else:
            if "JavaScript runtime" in result.stderr or "player_client" in result.stderr:
                cmd_fallback = [
                    "yt-dlp",
                    "-x",
                    "--audio-format", "mp3",
                    "--audio-quality", "5",
                    "--quiet",
                    "-o", str(output_path / f"{video_id}.%(ext)s"),
                    f"https://www.youtube.com/watch?v={video_id}",
                ]
                
                result_fallback = subprocess.run(cmd_fallback, capture_output=True, text=True, timeout=600)
                if result_fallback.returncode == 0:
                    for ext in ["mp3", "m4a", "opus", "wav", "webm"]:
                        audio_file = output_path / f"{video_id}.{ext}"
                        if audio_file.exists():
                            return str(audio_file)
        
        return None
    except subprocess.TimeoutExpired:
        return None
    except Exception as e:
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
        logging.error("openai-whisper not installed. Install with: pip install openai-whisper")
        return None
    except Exception as e:
        logging.error(f"Transcription error: {e}")
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
        logging.error("google-cloud-speech not installed")
        return None
    except Exception as e:
        logging.error(f"Transcription error: {e}")
        return None


def collect_speeches_for_speaker(
    speaker: Dict,
    max_speeches: int = 5,
    transcription_method: str = "whisper",
    existing_count: int = 0
) -> Dict:
    """Collect speeches by downloading audio and transcribing"""
    
    queries = []
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
    
    if existing_count == 0:
        name_parts = speaker['name'].split()
        if len(name_parts) >= 3:
            first_name = name_parts[-1]
            last_name = " ".join(name_parts[:-1])
            first_name_reverse = name_parts[0]
            last_name_reverse = " ".join(name_parts[1:])
            reversed_name = f"{name_parts[-1]} {' '.join(name_parts[:-1])}"
        elif len(name_parts) == 2:
            first_name = name_parts[0]
            last_name = name_parts[1]
            first_name_reverse = name_parts[1]
            last_name_reverse = name_parts[0]
            reversed_name = f"{name_parts[1]} {name_parts[0]}"
        else:
            first_name = name_parts[0] if name_parts else speaker['name']
            last_name = name_parts[0] if name_parts else speaker['name']
            first_name_reverse = first_name
            last_name_reverse = last_name
            reversed_name = speaker['name']
        
        generic_templates = [
            "{name} rally",
            "{reversed_name} rally",
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
            "{last_name} rally",
            "{last_name} speech",
            "{last_name} campaign speech",
            "{first_name} {last_name} rally",
            "{first_name} {last_name} speech",
            "{first_name_reverse} {last_name_reverse} rally",
            "{first_name_reverse} {last_name_reverse} speech",
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
                    "year": None
                })
            except KeyError:
                pass
    
    for year in CONFIG["search_years"]:
        for template in query_templates:
            queries.append({
                "query": template.format(name=speaker['name'], year=year),
                "year": year
            })
    
    queries = queries[:CONFIG["max_queries_per_speaker"]]
    collected = []
    speeches_found = 0
    
    # Create temp directory for audio files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        for query_info in queries:
            if speeches_found >= max_speeches:
                break
            
            query = query_info["query"]
            videos = search_youtube(query, max_results=CONFIG["max_videos_per_query"])
            
            if not videos:
                continue
            
            for video in videos:
                if speeches_found >= max_speeches:
                    break
                
                # Download audio
                audio_file = download_audio(video["video_id"], temp_path)
                if not audio_file:
                    continue
                
                delay = CONFIG.get("gap_filling_delay_between_downloads", CONFIG["delay_between_downloads"]) if CONFIG.get("gap_filling_mode", False) else CONFIG["delay_between_downloads"]
                time.sleep(delay + random.uniform(0, 2))
                
                # Transcribe
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
                    continue
                
                # For speakers with 0 speeches, be more lenient with transcript length
                if existing_count == 0 and CONFIG.get("gap_filling_mode", False):
                    min_length = CONFIG.get("gap_filling_min_transcript_length", 150)
                elif existing_count == 0:
                    min_length = max(200, CONFIG["min_transcript_length"] // 2)
                else:
                    min_length = CONFIG["min_transcript_length"]
                
                if len(transcript) < min_length:
                    continue
                
                speech_year = query_info["year"]
                if speech_year is None and video.get("publish_date"):
                    try:
                        speech_year = int(video["publish_date"][:4])
                    except:
                        speech_year = None
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
                
                collected.append(speech_data)
                speeches_found += 1
    
    return {"partisan_rally_speeches": collected}


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
        logging.info(f"Loaded {len(data)} speakers from collected_speeches.json")
    
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
        logging.info(f"Added {new_speakers} additional speakers from floor_speeches_congress_api.json")
    
    return list(speakers_dict.values())


def main():
    parser = argparse.ArgumentParser(description="Collect rally speeches via audio download + transcription")
    parser.add_argument("--max-per-person", type=int, default=3, help="Max speeches per person")
    parser.add_argument("--transcription-method", choices=["whisper", "google"], default="whisper",
                        help="Transcription method: whisper (free, local) or google (requires setup)")
    parser.add_argument("--test", action="store_true", help="Test with 2 speakers")
    parser.add_argument("--zero-only", action="store_true", help="Only process speakers with 0 speeches")
    parser.add_argument("--log-file", type=str, default=None, help="Log file path (optional)")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    
    # Enable gap-filling mode if requested
    if args.zero_only:
        CONFIG["gap_filling_mode"] = True
        logger.warning("Gap-filling mode enabled: Using longer delays to avoid rate limits")
    
    start_time = datetime.now()
    logger.info("="*60)
    logger.info("Rally Speech Collector - Audio Download + Transcription")
    logger.info("="*60)
    logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Transcription method: {args.transcription_method}")
    logger.info(f"Max speeches per person: {args.max_per_person}")
    logger.info("This script:")
    logger.info("1. Searches YouTube using official API (fast)")
    logger.info("2. Downloads audio using yt-dlp (bypasses rate limits)")
    logger.info("3. Transcribes using speech-to-text (no YouTube API needed)")
    
    # Check dependencies
    try:
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
        logger.info("yt-dlp installed")
    except:
        logger.error("yt-dlp not found. Install with: pip install yt-dlp")
        return
    
    # Check for ffmpeg (needed for audio extraction)
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        logger.info("ffmpeg installed")
    except:
        logger.warning("ffmpeg not found - audio extraction may fail")
        logger.info("  Install with: brew install ffmpeg (macOS) or apt-get install ffmpeg (Linux)")
    
    if args.transcription_method == "whisper":
        try:
            import whisper
            logger.info("openai-whisper installed")
        except ImportError:
            logger.error("openai-whisper not found. Install with: pip install openai-whisper")
            return
    
    # Check for search APIs
    if YOUTUBE_API_AVAILABLE:
        logger.info("YouTube Data API available (official)")
    else:
        logger.warning("YouTube Data API not available")
    
    if YOUTUBE_SEARCH_AVAILABLE:
        logger.info("youtubesearchpython available (fallback, no quota)")
    else:
        logger.warning("youtubesearchpython not installed - install with: pip install youtubesearchpython")
        logger.info("  (Fallback will not work if quota is exceeded)")
    
    # Load speakers
    logger.info("[1/3] Loading speaker roster...")
    speakers = load_speaker_roster()
    logger.info(f"Total unique speakers: {len(speakers)}")
    
    # Filter speakers
    if args.test:
        speakers = speakers[:2]
        logger.info(f"Test mode: processing {len(speakers)} speakers")
    
    # Load existing data
    output_path = Path(__file__).parent / CONFIG["output_json"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists():
        try:
            output = json.loads(output_path.read_text(encoding="utf-8"))
            existing_speakers = len(output)
            existing_speeches = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
            logger.info(f"Loaded existing data: {existing_speakers} speakers, {existing_speeches} speeches")
            
            # Skip speakers who already have enough speeches
            speakers_to_process = []
            for speaker in speakers:
                existing_count = len(output.get(speaker["key"], {}).get("partisan_rally_speeches", []))
                if args.zero_only:
                    if existing_count == 0:
                        speakers_to_process.append(speaker)
                elif existing_count < args.max_per_person:
                    speakers_to_process.append(speaker)
            
            if len(speakers_to_process) < len(speakers):
                skipped = len(speakers) - len(speakers_to_process)
                mode_str = "zero-only mode" if args.zero_only else "already have enough speeches"
                logger.info(f"Skipping {skipped} speakers ({mode_str})")
                speakers = speakers_to_process
        except Exception as e:
            logger.warning(f"Could not load existing data ({e}), starting fresh")
            output = {}
    else:
        output = {}
    
    # Collect speeches
    logger.info("[2/3] Collecting speeches...")
    for i, speaker in enumerate(speakers, 1):
        progress_pct = (i / len(speakers)) * 100
        logger.info(f"[{progress_pct:.1f}%] Processing {speaker['name']} ({i}/{len(speakers)})...")
        
        # Get existing count to pass to collection function
        existing_count = len(output.get(speaker["key"], {}).get("partisan_rally_speeches", []))
        
        result = collect_speeches_for_speaker(
            speaker,
            max_speeches=args.max_per_person,
            transcription_method=args.transcription_method,
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
            
            existing_count = len(output[speaker["key"]].get("partisan_rally_speeches", []))
            if new_speeches:
                logger.info(f"Collected {len(new_speeches)} new speeches (Total: {existing_count})")
            else:
                logger.info(f"No new speeches (Total: {existing_count})")
        else:
            existing_count = len(output[speaker["key"]].get("partisan_rally_speeches", []))
            logger.info(f"No speeches collected (Total: {existing_count})")
        
        # Save periodically
        if i % 10 == 0 or i == len(speakers):
            output_path.write_text(
                json.dumps(output, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            total = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
            logger.info(f"[Saved] {total} speeches from {len(output)} speakers")
        
        delay = CONFIG.get("gap_filling_delay_between_speakers", CONFIG["delay_between_speakers"]) if CONFIG.get("gap_filling_mode", False) else CONFIG["delay_between_speakers"]
        time.sleep(delay + random.uniform(0, 3))
    
    # Final save
    logger.info("[3/3] Final save...")
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    logger.info(f"Saved to {output_path}")
    
    # Summary
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds() / 60
    total_speeches = sum(len(data.get("partisan_rally_speeches", [])) for data in output.values())
    
    logger.info("="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total time: {total_time:.1f} minutes ({total_time/60:.2f} hours)")
    speakers_processed = len(speakers)
    logger.info(f"Speakers processed: {speakers_processed}/{speakers_processed}")
    logger.info(f"Total speakers in output: {len(output)}")
    logger.info(f"Total speeches collected: {total_speeches}")
    logger.info(f"Average per speaker: {total_speeches / len(output) if output else 0:.1f}")
    if speakers_processed > 0:
        logger.info(f"Average time per speaker: {total_time / speakers_processed:.1f} minutes")
    logger.info("="*60)


if __name__ == "__main__":
    main()

