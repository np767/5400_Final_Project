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
        """Clean and preprocess political speech transcripts for NLP analysis.

        Applies configurable text preprocessing operations to speech transcripts,
        including tokenization, normalization, and filtering. Designed to preserve
        important elements for political speech analysis (numbers, proper nouns,
        sentence structure) while removing noise and standardizing text format.

        Attributes:
            raw_data_folder (Path): Path to raw speech data directory.
            remove_stopwords (bool): Whether to remove common English stopwords.
            remove_urls (bool): Whether to remove URLs from text.
            remove_emails (bool): Whether to remove email addresses.
            remove_numbers (bool): Whether to remove numeric values.
            remove_punctuation (bool): Whether to remove punctuation marks.
            use_lowercase (bool): Whether to convert text to lowercase.
            check_token_len (bool): Whether to filter tokens by minimum length.
            min_token_len (int): Minimum token length when check_token_len is True.
            remove_extra_whitespace (bool): Whether to collapse multiple spaces.
            expand_contractions (bool): Whether to expand contractions (e.g., "don't" → "do not").
            remove_special_chars (bool): Whether to remove special characters.
        """

        if politician:
            self.raw_data_folder = RAW_DATA_DIR / Path(politician)
            folder_exists = ensure_politician_data_folder(politician, RAW_DATA_DIR)
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
        """Discover and load all speech transcript files for cleaning.

        Recursively searches the raw data folder for .txt files, reads their content,
        and prepares them for preprocessing. Maintains the organizational structure
        of politician and speech category information.

        Returns:
            None. Processes files in place and yields text content with metadata.

        Side Effects:
            - Prints progress information about discovered files
            - Reads speech files from disk
            - Extracts politician and category information from directory structure

        Note:
            Expects directory structure: raw_data_folder/politician/category/speech.txt
            Returns None if no .txt files are found in the specified directory.
            Uses UTF-8 encoding to handle special characters in transcripts.
        """
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
        """Apply configured preprocessing steps to clean speech transcript text.

        Executes a pipeline of text cleaning operations in a specific order designed
        to preserve important content while removing noise. Always applies encoding
        fixes and boilerplate removal, then applies optional transformations based
        on initialization parameters.

        Args:
            text (str): Raw speech transcript text to clean.

        Returns:
            str: Cleaned text with preprocessing operations applied.

        Processing Order:
            1. Fix encoding issues (always applied)
            2. Remove website boilerplate (always applied)
            3. Expand contractions (if enabled)
            4. Remove URLs (if enabled)
            5. Remove emails (if enabled)
            6. Remove special characters (if enabled)
            7. Remove numbers (if enabled)
            8. Convert to lowercase (if enabled)
            9. Remove punctuation (if enabled)
            10. Collapse extra whitespace (if enabled)

        Note:
            Processing order matters - for example, lowercasing before punctuation
            removal ensures consistent results. Encoding fixes and boilerplate
            removal always run first regardless of other settings.
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
        """Remove residual website navigation, headers, and footer content.

        Performs minimal boilerplate cleaning since most HTML elements are already
        stripped during the download stage by SpeechDownloader. This method catches
        any remaining website artifacts that may have slipped through initial cleaning.

        Args:
            text (str): Speech text that may contain residual boilerplate content.

        Returns:
            str: Text with website boilerplate removed.

        Note:
            Heavy lifting of boilerplate removal happens in SpeechDownloader.download_page()
            using BeautifulSoup to strip navigation elements. This method only handles
            edge cases and text-based patterns that survived initial HTML cleaning.
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
        """Fix common HTML encoding issues from web scraping.

        Corrects malformed UTF-8 characters and HTML entity encoding errors that
        commonly occur when scraping web pages. These artifacts typically result
        from incorrect character encoding interpretation during the download process.

        Args:
            text (str): Text containing potential encoding issues.

        Returns:
            str: Text with encoding issues corrected to standard ASCII/UTF-8 characters.

        Replacements:
            - â€™ → ' (apostrophe)
            - â€œ, â€ → " (quotation marks)
            - â€", â€" → —, - (dashes)
            - Â → (space)
            - Ã¢Â€Â™ → ' (apostrophe)
            - Ã¢Â€Â → " (quotation mark)
            - â€¦ → ... (ellipsis)

        Example:
            >>> cleaner.fix_encoding("Donâ€™t worry about it")
            "Don't worry about it"
        """

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
        """Execute complete text processing pipeline from cleaning to tokenization.

        Applies the full preprocessing workflow: cleans raw text, tokenizes into words,
        filters tokens based on configuration (stopwords, length), and optionally applies
        stemming or lemmatization. This is the main entry point for end-to-end text
        preprocessing.

        Args:
            text (str): Raw speech transcript text to process.

        Returns:
            List[str]: List of processed tokens ready for NLP analysis.

        Processing Pipeline:
            1. Clean text (via clean_text method)
            2. Tokenize into words
            3. Remove stopwords (if enabled)
            4. Filter by token length (if enabled)
            5. Apply stemming or lemmatization (if enabled)
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
        """Remove all numeric digits from text.

        Args:
            text (str): Text containing numbers.

        Returns:
            str: Text with all digits removed.
        """
        return re.sub(r"\d+", "", text)

    def emails(self, text: str) -> str:
        """Remove email addresses from text.

        Args:
            text (str): Text potentially containing email addresses.

        Returns:
            str: Text with email addresses removed.
        """
        return re.sub(r"\S+@\S+", "", text)

    def urls(self, text: str) -> str:
        """Remove URLs starting with http:// or https:// from text.

        Args:
            text (str): Text potentially containing URLs.

        Returns:
            str: Text with HTTP/HTTPS URLs removed.
        """
        return re.sub(r"https?://\S+", "", text)

    def make_lowercase(self, text: str) -> str:
        """Convert text to lowercase.

        Args:
            text (str): Text to convert.

        Returns:
            str: Lowercased text.
        """
        return text.lower()

    def punctuation(self, text: str) -> str:
        """Remove all punctuation from text.

        Args:
            text (str): Text containing punctuation.

        Returns:
            str: Text with punctuation removed.
        """
        return text.translate(str.maketrans("", "", string.punctuation))

    def special_chars(self, text: str) -> str:
        """Remove special characters, keeping only alphanumeric and spaces.

        More aggressive than punctuation removal - removes any character that
        isn't a letter, number, or space.

        Args:
            text (str): Text containing special characters.

        Returns:
            str: Text with only letters, numbers, and spaces.
        """
        return re.sub(r"[^a-zA-Z0-9\s]", "", text)

    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into words using NLTK's word_tokenize.

        Args:
            text (str): Text to tokenize.

        Returns:
            List[str]: List of word tokens.
        """
        return word_tokenize(text)

    def remove_stopwords_from_tokens(self, tokens: List[str]) -> List[str]:
        """Remove English stopwords from token list.

        Filters out common English words (the, is, at, etc.) that typically
        don't carry significant meaning for analysis.

        Args:
            tokens (List[str]): List of word tokens.

        Returns:
            List[str]: Filtered list with stopwords removed.
        """
        if not self.stop_words:
            return tokens
        return [token for token in tokens if token.lower() not in self.stop_words]

    def filter_by_length(self, tokens: List[str]) -> List[str]:
        """Filter tokens by minimum length threshold.

        Removes very short tokens that may not be meaningful for analysis.

        Args:
            tokens (List[str]): List of word tokens.

        Returns:
            List[str]: Tokens with length >= min_token_len.
        """
        return [token for token in tokens if len(token) >= self.min_token_len]

    def stem(self, tokens: List[str]) -> List[str]:
        """Apply Porter Stemming to reduce tokens to root form.

        Reduces words to their stem by removing suffixes (e.g., "running" → "run").
        Uses a rule-based approach that may produce non-dictionary words.

        Args:
            tokens (List[str]): List of word tokens.

        Returns:
            List[str]: List of stemmed tokens.
        """
        if not self.stemmer:
            return tokens
        return [self.stemmer.stem(token) for token in tokens]

    def lemmatize(self, tokens: List[str]) -> List[str]:
        """Apply lemmatization to reduce tokens to dictionary form.

        Reduces words to their base dictionary form (lemma) using vocabulary
        and morphological analysis. More accurate than stemming.

        Args:
            tokens (List[str]): List of word tokens.

        Returns:
            List[str]: List of lemmatized tokens.
        """
        if not self.lemmatizer:
            return tokens
        return [self.lemmatizer.lemmatize(token) for token in tokens]

    def preserve_sentences(self, text: str) -> List[str]:
        """Clean text while preserving sentence boundaries.

        Applies cleaning operations to each sentence independently, maintaining
        sentence structure for analyses that require sentence-level granularity.

        Args:
            text (str): Raw text to process.

        Returns:
            List[str]: List of cleaned sentences.
        """
        sentences = nltk.sent_tokenize(text)
        return [self.clean_text(sent) for sent in sentences]

    def get_statistics(self, text: str) -> Dict[str, Any]:
        """Calculate basic text statistics for exploratory data analysis.

        Computes various metrics about text complexity, length, and vocabulary
        diversity useful for understanding speech characteristics.

        Args:
            text (str): Text to analyze.

        Returns:
            Dict[str, Any]: Dictionary containing:
                - char_count: Total character count
                - word_count: Total word count
                - sentence_count: Total sentence count
                - avg_word_length: Average characters per word
                - unique_words: Number of unique words
                - lexical_diversity: Ratio of unique words to total words
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
        """Extract n-grams from tokens for pattern analysis.

        Creates sequences of n consecutive tokens, useful for detecting common
        phrases and rhetorical patterns in political speeches.

        Args:
            tokens (List[str]): List of word tokens.
            n (int, optional): Length of n-gram sequences. Defaults to 2 (bigrams).

        Returns:
            List[tuple]: List of n-gram tuples.
        """
        return list(nltk.ngrams(tokens, n))

    def get_pos_tags(self, tokens: List[str]) -> List[tuple]:
        """Get part-of-speech tags for tokens.

        Tags each token with its grammatical category (noun, verb, adjective, etc.),
        useful for analyzing rhetorical patterns and speech structure.

        Args:
            tokens (List[str]): List of word tokens.

        Returns:
            List[tuple]: List of (token, POS_tag) tuples.
        """
        return nltk.pos_tag(tokens)

    def get_word_frequencies(
        self, tokens: List[str], top_n: int = 20
    ) -> Dict[str, int]:
        """Get word frequency distribution for most common terms.

        Counts occurrences of each token and returns the most frequent ones,
        useful for identifying key themes and topics in speeches.

        Args:
            tokens (List[str]): List of word tokens.
            top_n (int, optional): Number of top words to return. Defaults to 20.

        Returns:
            Dict[str, int]: Dictionary mapping words to their frequencies, ordered
                by frequency (highest first).
        """
        return dict(Counter(tokens).most_common(top_n))

    def expand_contractions_text(self, text: str) -> str:
        """Expand contractions in text to full forms.

        Converts contracted forms to their expanded equivalents (e.g., "don't" → "do not"),
        useful for standardizing text and improving sentiment analysis accuracy.

        Args:
            text (str): Text containing contractions.

        Returns:
            str: Text with contractions expanded.
        """
        # Create pattern for case-insensitive matching
        pattern = re.compile(
            r"\b(" + "|".join(re.escape(key) for key in CONTRACTIONS.keys()) + r")\b",
            flags=re.IGNORECASE,
        )

        def replace(match):
            return CONTRACTIONS[match.group(0).lower()]

        return pattern.sub(replace, text)
