import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_collection.downloader import SpeechDownloader

if __name__ == "__main__":
    downloader = SpeechDownloader()
    downloader.download_all_speeches(download_file=True)
