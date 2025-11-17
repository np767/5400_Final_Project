from pathlib import Path
from typing import List, Dict, Optional
from preprocessing.config import SPEECH_CATEGORIES
import os


def ensure_politician_data_folder(politician_name: str) -> bool:
    try:
        project_root = Path(__file__).parent.parent
        data_path = Path(os.path.join(project_root, "data", politician_name))
        data_path.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print(f"Error creating directory: {e}")
        return False


def ensure_politician_raw_directories(politician_name: str, category: str) -> bool:
    try:
        main_folder_exists = ensure_politician_data_folder(politician_name)
        if not main_folder_exists:
            return False

        project_root = Path(__file__).parent.parent
        raw_path = Path(os.path.join(project_root, "data", politician_name))
        category_path = Path(os.path.join(raw_path, category))
        category_path.mkdir(parents=True, exist_ok=True)
        return True

    except Exception as e:
        print(f"Error creating category directories: {e}")
        return False


def ensure_politician_processing_directories(politician_name: str) -> bool:
    try:
        main_folder_exists = ensure_politician_data_folder(politician_name)
        if not main_folder_exists:
            return False

        project_root = Path(__file__).parent.parent
        main_politician_path = Path(os.path.join(project_root, "data", politician_name))

        for category in SPEECH_CATEGORIES:
            category_path = Path(
                os.path.join(main_politician_path, category, "_processed")
            )
            category_path.mkdir(parents=True, exist_ok=True)
        return True
    except:
        return False
