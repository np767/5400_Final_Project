from pathlib import Path

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

TIMEOUT = 30
SLEEP_TIME = 2
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CONFIG_DIR = DATA_DIR / "config"
SPEECH_URLS_FILE = CONFIG_DIR / "speech_urls.json"

if __name__ == "__main__":
    print(PROJECT_ROOT)
    print(DATA_DIR)
    print(CONFIG_DIR)
    print(SPEECH_URLS_FILE)
