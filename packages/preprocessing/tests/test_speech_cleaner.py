import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch
from preprocessing.cleaner import SpeechCleaner


@pytest.fixture
def temp_dirs():
    """Create temporary directories for raw and processed data"""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        raw_dir = temp_path / "raw"
        processed_dir = temp_path / "processed"
        raw_dir.mkdir()
        processed_dir.mkdir()

        # Create sample speech file
        politician_dir = raw_dir / "bernie_sanders" / "rally"
        politician_dir.mkdir(parents=True)

        speech_file = politician_dir / "test_speech.txt"
        speech_content = """
        This is a TEST speech! It contains URLs like https://example.com 
        and email addresses like test@example.com. Don't you think it's 
        interesting? We have numbers 123 and punctuation!!!
        
        â€™This has encoding issues.â€œ
        """
        speech_file.write_text(speech_content)

        yield raw_dir, processed_dir


def test_clean_text_file(temp_dirs):
    """Test that SpeechCleaner can clean a text file with various preprocessing steps"""
    raw_dir, processed_dir = temp_dirs

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

                # Process all speeches
                result = cleaner.obtain_texts_to_clean()

                # Check that processing completed
                assert result == 1

                # Check that output file was created
                output_file = (
                    processed_dir
                    / "bernie_sanders"
                    / "rally"
                    / "test_speech_processed.txt"
                )
                assert output_file.exists()

                # Read cleaned content
                cleaned_content = output_file.read_text()

                # Verify cleaning worked
                assert "https://example.com" not in cleaned_content  # URL removed
                assert "test@example.com" not in cleaned_content  # Email removed
                assert "123" not in cleaned_content  # Numbers removed
                assert "!!!" not in cleaned_content  # Punctuation removed
                assert cleaned_content.islower()  # Lowercase applied
                assert "do not" in cleaned_content  # Contractions expanded
                assert "â€™" not in cleaned_content  # Encoding fixed
