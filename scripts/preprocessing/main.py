import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.cleaner import SpeechCleaner

if __name__ == "__main__":
    cleaner = SpeechCleaner(
        remove_stopwords=False,
        remove_urls=True,
        remove_emails=True,
        remove_numbers=False,
        remove_punctuation=False,
        use_lowercase=True,
        expand_contractions=True,
        remove_special_chars=False,
    )
    cleaner.obtain_texts_to_clean()
