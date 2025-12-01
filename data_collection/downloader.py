import requests
from bs4 import BeautifulSoup
from pathlib import Path
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
    RAW_DATA_DIR,
    SPEECH_URLS_FILE,
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
        self.output_dirs = []

        with open(self.key_dir, "r") as file:
            data = json.load(file)
        self.politicians = list(data.keys())
        self.speeches = data

        for politician in self.politicians:
            output_dir = os.path.join(RAW_DATA_DIR, politician)
            folder_created = ensure_politician_data_folder(politician)
            if not folder_created:
                os.makedirs(output_dir, exist_ok=True)
            self.output_dirs.append(output_dir)

    def sanitize_filename(self, name: str) -> str:
        """Remove invalid characters from filename"""

        name = re.sub(r'[<>:"/\\|?*]', "", name)
        return name

    def save_transcript(self, output_dir, foldername, filename, text):
        filepath = os.path.join(output_dir, foldername, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)
        return True

    def download_page(
        self,
        url: str,
        foldername: str,
        filename: str,
        output_dir: str,
        download_file_regardless: bool = False,
    ) -> bool:
        """Download a webpage and save as text"""

        try:
            print(f"Downloading: {url}")
            filepath = os.path.join(output_dir, foldername, filename)

            if not os.path.exists(filepath) or download_file_regardless:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                for tag in soup(
                    [
                        "script",
                        "style",
                        "nav",
                        "header",
                        "footer",
                        "aside",
                        "form",
                        "button",
                        "iframe",
                    ]
                ):
                    tag.decompose()

                main_content = None
                content_selectors = [
                    ("id", "main-content"),
                    ("id", "content"),
                    ("id", "article"),
                    ("class", "entry-content"),
                    ("class", "article-content"),
                    ("class", "post-content"),
                    ("class", "article-body"),
                    ("role", "main"),
                    ("tag", "article"),
                    ("tag", "main"),
                ]

                for selector_type, selector_value in content_selectors:
                    if selector_type == "id":
                        main_content = soup.find(id=selector_value)
                    elif selector_type == "class":
                        main_content = soup.find(class_=selector_value)
                    elif selector_type == "role":
                        main_content = soup.find(attrs={"role": selector_value})
                    elif selector_type == "tag":
                        main_content = soup.find(selector_value)

                    if main_content:
                        break

                if not main_content:
                    main_content = soup.find("body") or soup

                text = main_content.get_text(separator="\n", strip=True)
                text = re.sub(r"\n{3,}", "\n\n", text)
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                text = "\n".join(lines)

                with open(filepath, "w", encoding="utf-8") as f:
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
        politician_summary_successful = {}
        politician_summary_fails = {}
        for politician in self.politicians:
            output_dir = [direc for direc in self.output_dirs if politician in direc][0]
            print(f"\nStarting download of {politician} speeches.")
            print(f"Output directory: {os.path.abspath(output_dir)}\n")

            successful = 0
            failed = 0

            for foldername, files in speeches[politician].items():
                folder_exists = ensure_politician_raw_directories(
                    politician, foldername
                )
                if not folder_exists:
                    folder_name = os.path.join(output_dir, foldername)
                    os.makedirs(folder_name, exist_ok=True)

                if isinstance(files, list):
                    for transcribe in files:
                        filename = transcribe.get("title", None)
                        transcript = transcribe.get("transcript", None)
                        if filename is not None and transcript is not None:
                            filename = str(filename).replace("/", "-").replace(":", "_")
                            filename = (
                                filename.lower().capitalize().replace(" ", "_") + ".txt"
                            )
                            saved_file = self.save_transcript(
                                output_dir, foldername, filename, transcript
                            )
                            if saved_file:
                                successful += 1
                else:
                    for filename, url in files.items():
                        if self.download_page(
                            url, foldername, filename, output_dir, download_file
                        ):
                            successful += 1
                        else:
                            failed += 1

                        time.sleep(self.sleep_time)
            politician_summary_successful[politician] = successful
            politician_summary_fails[politician] = failed

        print(f"\n{'='*80}")
        for index, politician in enumerate(politician_summary_successful.keys()):
            print(f"Download complete! - Politician: {politician}")
            print(f"Successful: {politician_summary_successful[politician]}")
            print(f"Failed: {politician_summary_fails[politician]}")
            print(f"Files saved to: {os.path.abspath(self.output_dirs[index])}")
            print(f"{'='*80}")
