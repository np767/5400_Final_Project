import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
PROCESSED_TOKENS_DIR = PROCESSED_DATA_DIR / "tokens"

SPEECH_CATEGORIES = [
    "partisan_rally_speeches",
    "senate_floor_speeches",
    "bipartisan_and_other_speeches",
]

# Preprocessing settings
REMOVE_STOPWORDS = True
PREPROCESSING_METHOD = "stem"

# Output format
SAVE_AS_TOKENS = True

# Text cleaning settings
REMOVE_URLS = True
REMOVE_EMAILS = True
REMOVE_NUMBERS = False
LOWERCASE = True
REMOVE_PUNCTUATION = True
MIN_TOKEN_LENGTH = 2

# File patterns
INPUT_FILE_EXTENSION = ".txt"
OUTPUT_FILE_EXTENSION = ".json" if SAVE_AS_TOKENS else ".txt"
