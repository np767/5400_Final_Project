# DSAN-5400 Natural Language Processing Final Project 

**Team Members:** Erin Brzusek, Nikhil Patla, Vinny Turtora, Mohammad Yassin and Sebastian Villalobos Alva

---

## Data Preprocessing Python Package

## Overview

This project examines rhetorical adaptation in political speech through computational linguistics. We analyze how figures like Bernie Sanders, JD Vance, and Ron DeSantis modify their language and sentiment based on audience and context.

## Architecture

```
speech_sources.json (URLs by politician/context)
           ↓
    SpeechDownloader (data_collection)
           ↓
    data/raw/{politician}/{category}/
           ↓
    SpeechReader → SpeechCleaner (preprocessing)
           ↓
    data/processed/{politician}/{category}/
           ↓
    Analysis (sentiment, EDA, comparison)
```

**Packages:**
- `data_collection`: Web scraping with `SpeechDownloader`
- `preprocessing`: Text processing with `SpeechReader` and `SpeechCleaner`
- `utils`: Shared helper functions

## Quick Start

### 1. Configure Sources
Edit `data/speech_sources.json`:
```json
{
  "bernie_sanders": {
    "rally": ["url1", "url2"],
    "senate_floor": ["url3"],
    "bipartisan": ["url4"]
  }
}
```

## Key Features

- **Flexible data collection** via JSON configuration
- **Robust preprocessing** that preserves proper nouns, numbers, and sentence structure
- **N-gram extraction** for linguistic pattern analysis
- **Clean separation** between data validation and text processing

## Project Structure

```
data/
├── raw/{politician}/{category}/          # Original speeches
├── processed/{politician}/{category}/    # Cleaned speeches
└── speech_sources.json                   # URL configuration

data_collection/
└── downloader.py                         # SpeechDownloader

preprocessing/
├── reader.py                             # SpeechReader
└── cleaner.py                            # SpeechCleaner
```

![Package Structure](package.png)

## Linguistic Features

Source: https://huggingface.co/stanfordnlp/stanza-en

## Sentiment Analysis

Source: https://huggingface.co/blog/sentiment-analysis-python