import os
import re
import json
from pathlib import Path
from typing import List, Dict, Optional
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords
from utils.helpers import ensure_politician_processing_directories

from .config import (
    DATA_DIR,
    REMOVE_STOPWORDS,
    USE_STEMMING,
    USE_LEMMATIZATION,
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

try:
    nltk.data.find("corpora/wordnet")
except LookupError:
    nltk.download("wordnet")


class SpeechCleaner:
    def __init__(
        self,
        politician: str,
        remove_stopwords: bool = REMOVE_STOPWORDS,
        remove_urls: bool = REMOVE_URLS,
        remove_emails: bool = REMOVE_EMAILS,
        remove_numbers: bool = REMOVE_NUMBERS,
        remove_punctuation: bool = REMOVE_PUNCTUATION,
        use_lowercase: bool = LOWERCASE,
        use_stemming: bool = USE_STEMMING,
        use_lemmatizaion: bool = USE_LEMMATIZATION,
        check_token_len: bool = False,
        min_token_len: int = MIN_TOKEN_LENGTH,
    ) -> None:
        self.raw_data_folder = os.path.join(DATA_DIR, politician)
        self.remove_stopwords = remove_stopwords
        self.remove_urls = remove_urls
        self.remove_emails = remove_emails
        self.remove_numbers = remove_numbers
        self.remove_punctuation = remove_punctuation
        self.use_lowercase = use_lowercase
        self.check_token_len = check_token_len
        self.min_token_len = min_token_len

        self.stemmer = PorterStemmer() if use_stemming else None
        self.lemmatizer = WordNetLemmatizer() if use_lemmatizaion else None

        folder_exists = ensure_politician_processing_directories(politician)
        if not folder_exists:
            pass

    def clean_text(self, text) -> None:
        pass

    def tokenize(self, text) -> None:
        pass

    def remove_stopwords_from_tokens(self, tokens) -> None:
        pass

    def stem(self, tokens) -> None:
        pass

    def lemmatize(self, tokens) -> None:
        pass
