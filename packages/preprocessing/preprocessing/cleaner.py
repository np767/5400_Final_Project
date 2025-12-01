import os
import re
import json
from collections import Counter
from pathlib import Path
from typing import List, Dict, Optional, Any
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords
from utils.helpers import (
    ensure_politician_data_folder,
)

from .config import (
    PROCESSED_DATA_DIR,
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
    CONTRACTIONS,
    RAW_DATA_DIR,
)

PROCESSED_TOKENS_DIR = PROCESSED_DATA_DIR / "tokens"

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
        politician: Optional[str] = None,
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
        remove_extra_whitespace: bool = True,
        expand_contractions: bool = False,
        remove_special_chars: bool = False,
    ):

        if politician:
            self.raw_data_folder = RAW_DATA_DIR / Path(politician)
            folder_exists = ensure_politician_data_folder(politician)
            if not folder_exists:
                raise ValueError(
                    f"{politician.capitalize()} raw directories do not exist"
                )
        else:
            self.raw_data_folder = RAW_DATA_DIR

        self.remove_stopwords = remove_stopwords
        self.remove_urls = remove_urls
        self.remove_emails = remove_emails
        self.remove_numbers = remove_numbers
        self.remove_punctuation = remove_punctuation
        self.use_lowercase = use_lowercase
        self.check_token_len = check_token_len
        self.min_token_len = min_token_len
        self.remove_extra_whitespace = remove_extra_whitespace
        self.expand_contractions = expand_contractions
        self.remove_special_chars = remove_special_chars

        self.stemmer = PorterStemmer() if use_stemming else None
        self.lemmatizer = WordNetLemmatizer() if use_lemmatizaion else None
        self.stop_words = set(stopwords.words("english")) if remove_stopwords else None

    def obtain_texts_to_clean(self):
        all_speeches = list(self.raw_data_folder.rglob("*.txt"))

        if not all_speeches:
            print(f"No .txt files found in {self.raw_data_folder}")
            return None

        print(f"Found {len(all_speeches)} speeches to process...")

        for i, speech in enumerate(all_speeches, 1):
            politician = speech.parent.parent.name
            category = speech.parent.name

            with open(speech, "r", encoding="utf-8") as f:
                raw_text = f.read()

            cleaned_text = self.clean_text(raw_text)

            output_dir = PROCESSED_DATA_DIR / politician / category
            output_dir.mkdir(parents=True, exist_ok=True)

            file_name = speech.stem + "_processed.txt"
            output_file = output_dir / file_name
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(cleaned_text)

        print(f"✓ Successfully processed and saved {len(all_speeches)} speeches")
        return len(all_speeches)

    def clean_text(self, text: str) -> str:
        """
        Clean text by applying various preprocessing steps.
        Returns cleaned text as a string.
        """
        text = self.fix_encoding(text)
        text = self.remove_boilerplate(text)

        if self.expand_contractions:
            text = self.expand_contractions_text(text)
        if self.remove_urls:
            text = self.urls(text)
        if self.remove_emails:
            text = self.emails(text)
        if self.remove_special_chars:
            text = self.special_chars(text)
        if self.remove_numbers:
            text = self.numbers(text)
        if self.use_lowercase:
            text = self.make_lowercase(text)
        if self.remove_punctuation:
            text = self.punctuation(text)
        if self.remove_extra_whitespace:
            text = " ".join(text.split())

        return text

    def remove_boilerplate(self, text: str) -> str:
        """
        Remove website navigation, headers, footers.
        Minimal cleaning since raw data is already clean from download.
        """

        text = re.sub(
            r"Prev\s*Previous.*?Next.*?Next", "", text, flags=re.IGNORECASE | re.DOTALL
        )
        footer_patterns = [
            r"Follow me on Twitter.*$",
            r"Office Locations.*$",
            r"Newsletter Signup.*$",
        ]

        for pattern in footer_patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.DOTALL)

        text = re.sub(
            r'^\s*(?:WASHINGTON|February|March|April).*?â€".*?\n',
            "",
            text,
            flags=re.MULTILINE,
            count=1,
        )

        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text

    def fix_encoding(self, text: str) -> str:
        """Fix common HTML encoding issues."""
        replacements = {
            "â€™": "'",
            "â€œ": '"',
            "â€": '"',
            'â€"': "—",
            'â€"': "-",
            "Â": " ",
            "Ã¢Â€Â™": "'",
            "Ã¢Â€Â": '"',
            "â€¦": "...",
        }
        for old, new in replacements.items():
            text = re.sub(old, new, text)
        return text

    def process_text(self, text: str) -> List[str]:
        """
        Complete processing pipeline: clean, tokenize, filter, and stem/lemmatize.
        Returns a list of processed tokens.
        """
        cleaned_text = self.clean_text(text)
        tokens = self.tokenize(cleaned_text)

        if self.remove_stopwords:
            tokens = self.remove_stopwords_from_tokens(tokens)
        if self.check_token_len:
            tokens = self.filter_by_length(tokens)

        if self.stemmer:
            tokens = self.stem(tokens)
        elif self.lemmatizer:
            tokens = self.lemmatize(tokens)

        return tokens

    def numbers(self, text: str) -> str:
        """Remove all numeric digits from text."""
        return re.sub(r"\d+", "", text)

    def emails(self, text: str) -> str:
        """Remove email addresses from text."""
        return re.sub(r"\S+@\S+", "", text)

    def urls(self, text: str) -> str:
        """Remove URLs starting with http:// or https:// from text."""
        return re.sub(r"https?://\S+", "", text)

    def make_lowercase(self, text: str) -> str:
        """Convert text to lowercase."""
        return text.lower()

    def punctuation(self, text: str) -> str:
        """Remove all punctuation from text."""
        return text.translate(str.maketrans("", "", string.punctuation))

    def special_chars(self, text: str) -> str:
        """
        Remove special characters, keeping only alphanumeric and spaces.
        More aggressive than just punctuation removal.
        """
        return re.sub(r"[^a-zA-Z0-9\s]", "", text)

    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into words using NLTK's word_tokenize.
        Returns a list of tokens.
        """
        return word_tokenize(text)

    def remove_stopwords_from_tokens(self, tokens: List[str]) -> List[str]:
        """
        Remove English stopwords from token list.
        Returns filtered list of tokens.
        """
        if not self.stop_words:
            return tokens
        return [token for token in tokens if token.lower() not in self.stop_words]

    def filter_by_length(self, tokens: List[str]) -> List[str]:
        """
        Filter tokens by minimum length.
        Returns tokens with length >= min_token_len.
        """
        return [token for token in tokens if len(token) >= self.min_token_len]

    def stem(self, tokens: List[str]) -> List[str]:
        """
        Apply Porter Stemming to tokens.
        Returns list of stemmed tokens.
        """
        if not self.stemmer:
            return tokens
        return [self.stemmer.stem(token) for token in tokens]

    def lemmatize(self, tokens: List[str]) -> List[str]:
        """
        Apply lemmatization to tokens.
        Returns list of lemmatized tokens.
        """
        if not self.lemmatizer:
            return tokens
        return [self.lemmatizer.lemmatize(token) for token in tokens]

    def preserve_sentences(self, text: str) -> List[str]:
        """
        Clean text but preserve sentence boundaries.
        Returns list of cleaned sentences - useful for sentiment analysis per sentence.
        """
        sentences = nltk.sent_tokenize(text)
        return [self.clean_text(sent) for sent in sentences]

    def get_statistics(self, text: str) -> Dict[str, Any]:
        """
        Get basic statistics about the text - useful for EDA.
        Returns dictionary with various metrics.
        """
        tokens = self.tokenize(text)

        return {
            "char_count": len(text),
            "word_count": len(tokens),
            "sentence_count": len(nltk.sent_tokenize(text)),
            "avg_word_length": (
                sum(len(token) for token in tokens) / len(tokens) if tokens else 0
            ),
            "unique_words": len(set(tokens)),
            "lexical_diversity": len(set(tokens)) / len(tokens) if tokens else 0,
        }

    def extract_ngrams(self, tokens: List[str], n: int = 2) -> List[tuple]:
        """
        Extract n-grams from tokens for pattern analysis.
        Useful for detecting common phrases in speeches.
        """
        return list(nltk.ngrams(tokens, n))

    def get_pos_tags(self, tokens: List[str]) -> List[tuple]:
        """
        Get part-of-speech tags for tokens.
        Useful for analyzing speech patterns (e.g., use of adjectives, verbs).
        """
        return nltk.pos_tag(tokens)

    def get_word_frequencies(
        self, tokens: List[str], top_n: int = 20
    ) -> Dict[str, int]:
        """
        Get word frequency distribution.
        Useful for EDA and identifying key terms.
        """
        return dict(Counter(tokens).most_common(top_n))

    def expand_contractions_text(self, text: str) -> str:
        """
        Expand contractions in text (e.g., "don't" -> "do not").
        Useful for sentiment analysis and pattern detection.
        """
        # Create pattern for case-insensitive matching
        pattern = re.compile(
            r"\b(" + "|".join(re.escape(key) for key in CONTRACTIONS.keys()) + r")\b",
            flags=re.IGNORECASE,
        )

        def replace(match):
            return CONTRACTIONS[match.group(0).lower()]

        return pattern.sub(replace, text)
