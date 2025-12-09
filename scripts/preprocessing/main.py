import sys
import argparse
from pathlib import Path
from preprocessing.cleaner import SpeechCleaner

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean and preprocess political speeches"
    )

    parser.add_argument(
        "--remove-stopwords", action="store_true", help="Remove stopwords from text"
    )
    parser.add_argument(
        "--keep-urls", action="store_true", help="Keep URLs in text (default: remove)"
    )
    parser.add_argument(
        "--keep-emails",
        action="store_true",
        help="Keep emails in text (default: remove)",
    )
    parser.add_argument(
        "--remove-numbers", action="store_true", help="Remove numbers from text"
    )
    parser.add_argument(
        "--remove-punctuation", action="store_true", help="Remove punctuation from text"
    )
    parser.add_argument(
        "--keep-case",
        action="store_true",
        help="Keep original case (default: lowercase)",
    )
    parser.add_argument(
        "--no-expand-contractions",
        action="store_true",
        help="Don't expand contractions (default: expand)",
    )
    parser.add_argument(
        "--remove-special-chars",
        action="store_true",
        help="Remove special characters from text",
    )
    args = parser.parse_args()

    cleaner = SpeechCleaner(
        remove_stopwords=args.remove_stopwords,
        remove_urls=not args.keep_urls,
        remove_emails=not args.keep_emails,
        remove_numbers=args.remove_numbers,
        remove_punctuation=args.remove_punctuation,
        use_lowercase=not args.keep_case,
        expand_contractions=not args.no_expand_contractions,
        remove_special_chars=args.remove_special_chars,
    )
    cleaner.obtain_texts_to_clean()
