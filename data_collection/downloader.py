import requests
from bs4 import BeautifulSoup
from utils.helpers import (
    ensure_politician_raw_directories,
    ensure_politician_data_folder,
)
import os
import time
import re
import json

from .config import (
    DEFAULT_HEADERS,
    TIMEOUT,
    SLEEP_TIME,
    DATA_DIR,
    SPEECH_URLS_FILE,
    Path,
)


class SpeechDownloader:
    def __init__(
        self,
        output_dir: str | None = None,
        key_dir: str | Path = SPEECH_URLS_FILE,
        headers=None,
        timeout: int = TIMEOUT,
        sleep_time: int = SLEEP_TIME,
    ):
        self.headers = headers if headers is not None else DEFAULT_HEADERS
        self.timeout = timeout
        self.sleep_time = sleep_time
        self.key_dir = key_dir

        with open(self.key_dir, "r") as file:
            data = json.load(file)
        self.politician = list(data.keys())[0]
        self.speeches = data[self.politician]

        self.output_dir = output_dir or os.path.join(DATA_DIR, self.politician)
        folder_created = ensure_politician_data_folder(self.politician)
        if not folder_created:
            os.makedirs(self.output_dir, exist_ok=True)

    def sanitize_filename(self, name: str) -> str:
        """Remove invalid characters from filename"""

        name = re.sub(r'[<>:"/\\|?*]', "", name)
        return name

    def download_page(
        self,
        url: str,
        foldername: str,
        filename: str,
        download_file_regardless: bool = False,
    ) -> bool:
        """Download a webpage and save as text"""

        try:
            print(f"Downloading: {url}")
            filepath = os.path.join(self.output_dir, foldername, filename)

            if not os.path.exists(filepath) or download_file_regardless:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                for tag in soup(["script", "style", "nav", "header", "footer"]):
                    tag.decompose()

                text = soup.get_text()
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"Source URL: {url}\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(text)

                print(f"✓ Saved: {filename}")
                return True
            else:
                print(f"File - {filepath} already exists")
                return True

        except Exception as e:
            print(f"✗ Error downloading {url}: {str(e)}")
            return False

    def download_all_speeches(self, download_file: bool = False):
        """Download all speeches"""

        speeches = self.speeches
        print(f"Starting download of {len(speeches)} speeches...")
        print(f"Output directory: {os.path.abspath(self.output_dir)}\n")

        successful = 0
        failed = 0

        for foldername, files in speeches.items():
            folder_exists = ensure_politician_raw_directories(
                self.politician, foldername
            )
            if not folder_exists:
                folder_name = os.path.join(self.output_dir, foldername)
                os.makedirs(folder_name, exist_ok=True)

            for filename, url in files.items():
                if self.download_page(url, foldername, filename, download_file):
                    successful += 1
                else:
                    failed += 1

                time.sleep(self.sleep_time)

        print(f"\n{'='*80}")
        print(f"Download complete!")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Files saved to: {os.path.abspath(self.output_dir)}")
        print(f"{'='*80}")
