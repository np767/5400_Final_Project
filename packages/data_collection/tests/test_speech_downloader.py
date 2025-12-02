import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from data_collection.downloader import SpeechDownloader


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_json_file(temp_dir):
    """Create a sample JSON file with speech data"""
    json_data = {
        "test_politician": {
            "rally": [
                {
                    "title": "Test Rally Speech",
                    "transcript": "This is a test transcript from a rally.",
                }
            ]
        }
    }
    json_path = temp_dir / "test_speeches.json"
    with open(json_path, "w") as f:
        json.dump(json_data, f)
    return json_path


def test_download_from_json(temp_dir, sample_json_file):
    """Test that SpeechDownloader can read and process JSON transcripts"""

    # Mock that creates actual directories when called
    def mock_ensure_dirs(politician, category, raw_data_dir):
        cat_dir = raw_data_dir / politician / category
        cat_dir.mkdir(parents=True, exist_ok=True)
        return False

    with patch(
        "data_collection.downloader.ensure_politician_data_folder", return_value=True
    ):
        with patch("data_collection.downloader.RAW_DATA_DIR", temp_dir):
            with patch(
                "data_collection.downloader.ensure_politician_raw_directories",
                side_effect=mock_ensure_dirs,
            ):
                downloader = SpeechDownloader(key_dir=sample_json_file)
                downloader.download_all_speeches()

                # Check that the transcript was saved
                output_file = (
                    temp_dir / "test_politician" / "rally" / "Test_rally_speech.txt"
                )
                assert output_file.exists()
                content = output_file.read_text()
                assert "This is a test transcript from a rally." in content


@patch("data_collection.downloader.requests.get")
def test_download_from_html(mock_get, temp_dir):
    """Test that SpeechDownloader can scrape and save HTML pages"""
    # Mock HTML response
    mock_response = Mock()
    mock_response.text = """
    <html>
        <body>
            <nav>Skip navigation</nav>
            <main>
                <p>This is the main speech content.</p>
                <p>It has multiple paragraphs.</p>
            </main>
            <footer>Footer content</footer>
        </body>
    </html>
    """
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    # Create JSON with URL
    json_data = {
        "test_politician": {
            "senate_floor": {"test_speech.txt": "https://example.com/speech"}
        }
    }
    json_path = temp_dir / "test_speeches.json"
    with open(json_path, "w") as f:
        json.dump(json_data, f)

    # Mock that creates actual directories when called
    def mock_ensure_dirs(politician, category, raw_data_dir):
        cat_dir = raw_data_dir / politician / category
        cat_dir.mkdir(parents=True, exist_ok=True)
        return False

    with patch(
        "data_collection.downloader.ensure_politician_data_folder", return_value=True
    ):
        with patch("data_collection.downloader.RAW_DATA_DIR", temp_dir):
            with patch(
                "data_collection.downloader.ensure_politician_raw_directories",
                side_effect=mock_ensure_dirs,
            ):
                with patch("data_collection.downloader.time.sleep"):
                    downloader = SpeechDownloader(key_dir=json_path)
                    downloader.download_all_speeches()

                    # Check that HTML was scraped and saved
                    output_file = (
                        temp_dir
                        / "test_politician"
                        / "senate_floor"
                        / "test_speech.txt"
                    )
                    assert output_file.exists()
                    content = output_file.read_text()

                    # Main content should be present
                    assert "This is the main speech content" in content
                    assert "It has multiple paragraphs" in content

                    # Navigation and footer should be removed
                    assert "Skip navigation" not in content
                    assert "Footer content" not in content
