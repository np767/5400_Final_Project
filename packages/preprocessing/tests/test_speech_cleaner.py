import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
from preprocessing.cleaner import SpeechCleaner

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_data_dirs(tmp_path):
    """Create mock raw and processed directories with sample speech"""
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()

    # Create politician directory structure
    politician_dir = raw_dir / "bernie_sanders" / "rally"
    politician_dir.mkdir(parents=True)

    # Copy sample speech from fixtures
    sample_speech = FIXTURES_DIR / "sample_speech.txt"
    speech_file = politician_dir / "test_speech.txt"
    shutil.copy(sample_speech, speech_file)

    yield raw_dir, processed_dir


def test_clean_text_file(mock_data_dirs):
    """Test that SpeechCleaner can clean a text file with various preprocessing steps"""
    raw_dir, processed_dir = mock_data_dirs

    with patch("preprocessing.cleaner.RAW_DATA_DIR", raw_dir):
        with patch("preprocessing.cleaner.PROCESSED_DATA_DIR", processed_dir):
            with patch(
                "preprocessing.cleaner.ensure_politician_data_folder",
                return_value=True,
            ):
                # Initialize cleaner with common preprocessing options
                cleaner = SpeechCleaner(
                    politician="bernie_sanders",
                    remove_urls=True,
                    remove_emails=True,
                    remove_numbers=True,
                    remove_punctuation=True,
                    use_lowercase=True,
                    expand_contractions=True,
                )

                result = cleaner.obtain_texts_to_clean()
                assert result == 1

                output_file = (
                    processed_dir
                    / "bernie_sanders"
                    / "rally"
                    / "test_speech_processed.txt"
                )
                assert output_file.exists()
                cleaned_content = output_file.read_text()

                assert "https://example.com" not in cleaned_content  # URL removed
                assert "test@example.com" not in cleaned_content  # Email removed
                assert "123" not in cleaned_content  # Numbers removed
                assert "!!!" not in cleaned_content  # Punctuation removed
                assert cleaned_content.islower()  # Lowercase applied
                assert "do not" in cleaned_content  # Contractions expanded
                assert "â€™" not in cleaned_content  # Encoding fixed
