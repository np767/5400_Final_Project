import sys
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_collection.downloader import SpeechDownloader
from data_collection.config import CONFIG_DIR

COLLECTED_SPEECHES = CONFIG_DIR / "collected_speeches.json"
FLOOR_SPEECHES = CONFIG_DIR / "floor_speeches_congress_api.json"
RALLY_SPEECHES_1 = CONFIG_DIR / "rally_speeches_youtube.json"
RALLY_SPEECHES_2 = CONFIG_DIR / "rally_speeches_audio_transcribed.json"

CONFIG_MAP = {
    "collected": COLLECTED_SPEECHES,
    "floor": FLOOR_SPEECHES,
    "rally1": RALLY_SPEECHES_1,
    "rally2": RALLY_SPEECHES_2,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download political speeches")
    parser.add_argument(
        "--config",
        type=str,
        choices=list(CONFIG_MAP.keys()),
        required=True,
        help="Configuration file to use (collected, floor, rally1, rally2)",
    )
    args = parser.parse_args()

    key_dir = CONFIG_MAP[args.config]
    downloader = SpeechDownloader(key_dir=key_dir)
    downloader.download_all_speeches(download_file=True)
