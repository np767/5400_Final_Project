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
from typing import Dict
from .config import DEFAULT_HEADERS, TIMEOUT, SLEEP_TIME, RAW_DATA_DIR, SPEECH_URLS_FILE


class SpeechDownloader:
    def __init__(
        self,
        output_dir: str | None = None,
        key_dir: str | Path = SPEECH_URLS_FILE,
        headers: Dict | None = None,
        timeout: int = TIMEOUT,
        sleep_time: int = SLEEP_TIME,
    ):
        """Downloads and manages political speech data from specified URLs or YouTube API transcribes.

        This class handles the retrieval of speech transcripts from web sources,
        organizing them by politician and maintaining a structured directory hierarchy
        under the raw data folder.

        Attributes:
            headers (dict): HTTP headers for web requests.
            timeout (int): Request timeout in seconds.
            sleep_time (int): Delay between requests in seconds to avoid rate limiting.
            key_dir (str | Path): Path to JSON file containing speech URLs.
            output_dirs (list[str]): List of output directories for each politician.
        """
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
            folder_created = ensure_politician_data_folder(politician, RAW_DATA_DIR)
            if not folder_created:
                os.makedirs(output_dir, exist_ok=True)
            self.output_dirs.append(output_dir)

    def sanitize_filename(self, name: str) -> str:
        """Remove invalid characters from filename to ensure cross-platform compatibility.

        Strips characters that are not allowed in filenames,
        preventing errors during file creation. This is particularly important for
        speech titles that may contain special characters or punctuation.

        Args:
            name (str): Raw filename or title string that may contain invalid characters.

        Note:
            Removes the following Windows-invalid characters: < > : " / \\ | ? *
            Does not collapse multiple spaces or trim whitespace.

        Returns:
            str: Sanitized filename with Windows-restricted characters removed.
                Preserves spaces, hyphens, underscores, periods, and all alphanumeric
                characters.

        Example:
            >>> downloader.sanitize_filename("Speech: Climate Change (2024)")
            "Speech Climate Change (2024)"

            >>> downloader.sanitize_filename('Senator\'s "Remarks" on Trade')
            "Senator's Remarks on Trade"
        """
        name = re.sub(r'[<>:"/\\|?*]', "", name)
        return name

    def save_transcript(self, output_dir, foldername, filename, text):
        """Save a speech transcript in the appropriate directory structure.

        Writes the transcript text to a file organized by politician and speech category,
        maintaining the project's data hierarchy (e.g., data/raw/politician/category/speech.txt).

        Args:
            output_dir (str): Base output directory for the politician (e.g., "data/raw/bernie_sanders").
            foldername (str): Category subdirectory name (e.g., "partisan_rally", "senate_floor").
            filename (str): Name of the file to save (should be pre-sanitized).
            text (str): Full transcript text content to write to file.

        Returns:
            bool: True if the file was successfully written.

        Example:
            >>> downloader.save_transcript(
            ...     "data/raw/bernie_sanders",
            ...     "partisan_rally",
            ...     "healthcare_speech_2024.txt",
            ...     "Thank you for coming today..."
            ... )
            True
        """
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
        """Download a webpage, extract clean text content, and save to disk.

        Fetches a speech transcript from a URL, strips HTML navigation and boilerplate
        elements using BeautifulSoup, extracts the main content, and saves the cleaned
        text. This method handles the data collection stage cleaning to avoid processing
        website navigation and metadata in later analysis stages.

        Args:
            url (str): Full URL of the webpage to download.
            foldername (str): Category subdirectory name (e.g., "partisan_rally", "senate_floor").
            filename (str): Name for the saved file (should be pre-sanitized).
            output_dir (str): Base output directory for the politician (e.g., "data/raw/bernie_sanders").
            download_file_regardless (bool, optional): If True, re-download even if file exists.
                Defaults to False (skip existing files).

        Returns:
            bool: True if download and save succeeded or file already exists, False if error occurred.

        Note:
            - Removes HTML elements: script, style, nav, header, footer, aside, form, button, iframe
            - Attempts to find main content using common selectors (main-content, article, etc.)
            - Falls back to <body> if no main content container found
            - Collapses multiple consecutive newlines to maximum of two
            - Strips empty lines and extra whitespace
            - Uses UTF-8 encoding to preserve special characters
            - Respects existing files unless download_file_regardless=True
        """

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
        """Download all speech transcripts for all politicians from configured URLs.

        Iterates through all politicians and their speech categories defined in the
        JSON configuration file, downloading each speech and organizing files by
        politician and category. Tracks success/failure counts and ensures proper
        directory structure exists before downloading.

        Args:
            download_file (bool, optional): If True, re-download files that already exist.
                If False, skip existing files. Defaults to False.

        Returns:
            None. Prints progress and summary statistics to console.

        Side Effects:
            - Creates directory structure under RAW_DATA_DIR if needed
            - Downloads and saves speech files to disk
            - Prints download progress and results for each politician
            - Respects self.sleep_time delay between requests

        Note:
            Uses ensure_politician_raw_directories() to check for existing folders
            before creating them. Downloads are organized as: RAW_DATA_DIR/politician/category/speech.txt
        """
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
                    politician, foldername, RAW_DATA_DIR
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
