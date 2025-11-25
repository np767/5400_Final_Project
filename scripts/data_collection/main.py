import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_collection.downloader import SpeechDownloader
from data_collection.config import CONFIG_DIR

COLLECTED_SPEECHES = CONFIG_DIR / "collected_speeches.json"

if __name__ == "__main__":
    downloader = SpeechDownloader()
    downloader.download_all_speeches(download_file=True)

    downloader = SpeechDownloader(key_dir=COLLECTED_SPEECHES)
    downloader.download_all_speeches(download_file=True)
