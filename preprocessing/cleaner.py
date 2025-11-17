import os
import re
import json
from pathlib import Path
from typing import List, Dict, Optional
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

from .config import (
    DATA_DIR,
    REMOVE_STOPWORDS,
    PREPROCESSING_METHOD,
    SAVE_AS_TOKENS,
    REMOVE_URLS,
    REMOVE_EMAILS,
    REMOVE_NUMBERS,
    LOWERCASE,
    REMOVE_PUNCTUATION,
    MIN_TOKEN_LENGTH,
)

# Download NLTK data
try:
    nltk.data.find("tokenizers/punkt")
except LookupError:
    nltk.download("punkt")

try:
    nltk.data.find("corpora/stopwords")
except LookupError:
    nltk.download("stopwords")


class SpeechCleaner:
    def __init__(
        self,
        politician_folder_name: str,
        remove_stopwords: bool = REMOVE_STOPWORDS,
        remove_urls: bool = REMOVE_URLS,
        remove_emails: bool = REMOVE_EMAILS,
        remove_numbers: bool = REMOVE_NUMBERS,
        remove_punctuation: bool = REMOVE_PUNCTUATION,
        use_lowercase: bool = LOWERCASE,
        preprocess_method: str = PREPROCESSING_METHOD,
        check_token_len: bool = False,
        min_token_len: int = MIN_TOKEN_LENGTH,
    ) -> None:
        self.raw_data_folder = os.path.join(DATA_DIR, politician_folder_name)

        pass
