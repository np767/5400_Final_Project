# DSAN-5400 Natural Language Processing Final Project 

**Team Members:** Erin Brzusek, Vinny Turtora, Sebastian Villalobos, Mohammad Yassin and Nikhil Patla

---

## Overview

This project examines rhetorical adaptation in political speech through computational linguistics. We analyze how figures like Bernie Sanders, JD Vance, and Ron DeSantis modify their language and sentiment based on audience and context.

## Installation

### Prerequisites
- Python 3.10 or higher
- Poetry (recommended)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/np767/5400_Final_Project.git
cd 5400_Final_Project
```

2. **Install the packages**

Using conda (recommended):
```bash
pip install -e packages/data_collection
pip install -e packages/preprocessing
pip install -e packages/utils
```

Using Poetry (for development):
```bash
cd packages/data_collection && poetry install && cd ../..
cd packages/preprocessing && poetry install && cd ../..
cd packages/utils && poetry install && cd ../..
```

3. **Download NLTK data** (if not already installed)
```python
import nltk
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')
```

### Verify Installation
```python
from data_collection import SpeechDownloader
from preprocessing import SpeechCleaner

print("Installation successful!")
```

## Architecture
```
speech_sources.json (URLs by politician/context)
           ↓
    SpeechDownloader (data_collection)
           ↓
    data/raw/{politician}/{category}/
           ↓
    SpeechCleaner (preprocessing)
           ↓
    data/processed/{politician}/{category}/
           ↓
    Analysis (sentiment, EDA, comparison)
```

**Packages:**
- `data_collection`: Web scraping with `SpeechDownloader`
- `preprocessing`: Text processing with `SpeechCleaner`
- `utils`: Shared helper functions

## Quick Start

### 1. Configure Sources
Edit `data/config/speech_sources.json`:
```json
{
  "bernie_sanders": {
    "rally": ["url1", "url2"],
    "senate_floor": ["url3"],
    "bipartisan": ["url4"]
  }
}
```

### 2. Collect Data

**Using the command line:**
```bash
# Download speeches from specific configuration files
python packages/data_collection/data_collection/main.py --config collected
python packages/data_collection/data_collection/main.py --config floor
python packages/data_collection/data_collection/main.py --config rally1
python packages/data_collection/data_collection/main.py --config rally2
```

### 3. Preprocess Speeches

**Using the command line:**
```bash
python packages/preprocessing/preprocessing/main.py # Use default preprocessing settings
python packages/preprocessing/preprocessing/main.py --remove-stopwords --remove-numbers # Remove stopwords and numbers (preserve proper nouns)
python packages/preprocessing/preprocessing/main.py --keep-urls --keep-case # Keep URLs and original case
python packages/preprocessing/preprocessing/main.py --remove-stopwords --remove-numbers --remove-punctuation --remove-special-chars # Minimal cleaning (remove stopwords, numbers, punctuation, special chars)
```

**Preprocessing options:**

- `--remove-stopwords`: Remove common stopwords from text
- `--keep-urls`: Preserve URLs in text (default: remove)
- `--keep-emails`: Preserve email addresses (default: remove)
- `--remove-numbers`: Remove all numbers from text
- `--remove-punctuation`: Remove punctuation marks
- `--keep-case`: Preserve original capitalization (default: lowercase)
- `--no-expand-contractions`: Don't expand contractions like "don't" → "do not"
- `--remove-special-chars`: Remove special characters

**Default settings:**

- URLs and emails removed
- Numbers and punctuation preserved (important for political analysis)
- Text lowercased
- Contractions expanded
- Stopwords and special characters kept

**Using Python:**

```python
from preprocessing import SpeechCleaner

cleaner = SpeechCleaner()
cleaned = cleaner.obtain_texts_to_clean() # Read Speeches & Clean text
```

## Project Structure
```
5400_Final_Project/
├── packages/
│   ├── data_collection/        # Speech collection tools
│   ├── preprocessing/          # Text processing tools
│   └── utils/                  # Shared utilities
├── data/
│   ├── raw/                    # Original speeches
│   ├── processed/              # Cleaned speeches
│   └── config/                 # Configuration files
├── analysis/                   # Analysis notebooks
└── scripts/                    # Standalone scripts
```

![Package Structure](package.png)

## Development

### Running Tests
```bash
cd packages/data_collection && poetry run pytest
cd packages/preprocessing && poetry run pytest
cd packages/utils && poetry run pytest
```

### Package Dependencies

## Linguistic Features

Source: https://huggingface.co/stanfordnlp/stanza-en

## Sentiment Analysis

Source: https://huggingface.co/blog/sentiment-analysis-python