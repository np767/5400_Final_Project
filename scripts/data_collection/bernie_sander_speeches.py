import requests
from bs4 import BeautifulSoup
import os
import time
import re
import json

OUTPUT_DIR = "../../data/sanders_bernie"
SPEECHES_KEY = "speeches_key/bernie_sanders_speeches_key_file.json"


class SpeechDownloader:
    def __init__(self, output_dir: str, key_dir: str):
        self.output_dir = output_dir
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        with open(key_dir, "r") as file:
            self.speeches = json.load(file)

        os.makedirs(output_dir, exist_ok=True)

    def sanitize_filename(self, name: str) -> str:
        """Remove invalid characters from filename"""

        name = re.sub(r'[<>:"/\\|?*]', "", name)
        return name

    def download_page(self, url: str, foldername: str, filename: str) -> bool:
        """Download a webpage and save as text"""

        try:
            print(f"Downloading: {url}")
            filepath = os.path.join(self.output_dir, foldername, filename)

            if not os.path.exists(filepath):
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                for tag in soup(["script", "style", "nav", "header", "footer"]):
                    tag.decompose()

                text = soup.get_text(separator="\n", strip=True)
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

    def download_all_speeches(self):
        """Download all Bernie Sanders speeches"""

        speeches = self.speeches
        print(f"Starting download of {len(speeches)} speeches...")
        print(f"Output directory: {os.path.abspath(self.output_dir)}\n")

        successful = 0
        failed = 0

        for foldername, files in speeches.items():
            folder_name = os.path.join(OUTPUT_DIR, foldername)
            os.makedirs(folder_name, exist_ok=True)

            for filename, url in files.items():
                if self.download_page(url, foldername, filename):
                    successful += 1
                else:
                    failed += 1

                time.sleep(2)

        print(f"\n{'='*80}")
        print(f"Download complete!")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Files saved to: {os.path.abspath(self.output_dir)}")
        print(f"{'='*80}")


if __name__ == "__main__":
    downloader = SpeechDownloader(output_dir=OUTPUT_DIR, key_dir=SPEECHES_KEY)
    downloader.download_all_speeches()
