import pytest
import json
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
from data_collection.downloader import SpeechDownloader

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_json_mixed():
    """Load the test speeches JSON file"""
    return FIXTURES_DIR / "test_speeches.json"


def test_json_loads(sample_json_mixed):
    """Test that JSON file loads correctly"""
    with open(sample_json_mixed, "r") as f:
        data = json.load(f)

    assert "test_politician" in data
    assert "rally" in data["test_politician"]
    assert "senate_floor" in data["test_politician"]


def create_mock_html_response():
    """Create a mock HTTP response with HTML content for testing"""
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
    return mock_response


def test_speech_downloader_saves_files(sample_json_mixed):
    """Test that SpeechDownloader saves transcript and HTML files correctly"""

    mock_raw_dir = FIXTURES_DIR / "mock_raw_data"

    if mock_raw_dir.exists():
        shutil.rmtree(mock_raw_dir)
    mock_raw_dir.mkdir(parents=True)

    # Override the raw data directory BEFORE initializing downloader
    from data_collection import (
        downloader as downloader_module,
    )

    original_raw_dir = downloader_module.RAW_DATA_DIR
    downloader_module.RAW_DATA_DIR = mock_raw_dir

    try:
        mock_response = create_mock_html_response()
        with patch(
            "data_collection.downloader.requests.get", return_value=mock_response
        ):
            with patch("data_collection.downloader.time.sleep"):
                downloader = SpeechDownloader(key_dir=sample_json_mixed)
                downloader.download_all_speeches(download_file=True)

        rally_file = (
            mock_raw_dir / "test_politician" / "rally" / "Test_rally_speech.txt"
        )
        assert rally_file.exists(), "Rally speech file was not created"
        rally_content = rally_file.read_text()
        assert "This is a test transcript from a rally." in rally_content

        senate_file = (
            mock_raw_dir / "test_politician" / "senate_floor" / "test_speech.txt"
        )
        assert senate_file.exists(), "Senate speech file was not created"
        senate_content = senate_file.read_text()

        assert "This is the main speech content." in senate_content
        assert "It has multiple paragraphs." in senate_content

        assert "<nav>" not in senate_content
        assert "<footer>" not in senate_content
        assert "<html>" not in senate_content

        assert "Skip navigation" not in senate_content
        assert "Footer content" not in senate_content

    finally:
        downloader_module.RAW_DATA_DIR = original_raw_dir
        if mock_raw_dir.exists():
            shutil.rmtree(mock_raw_dir)
