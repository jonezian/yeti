# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Yeti is a real-time Bluesky post monitor that connects to the Jetstream WebSocket service. It filters posts by keywords and automatically translates non-English content to English using Google Translate.

## Running the Application

```bash
# Activate virtual environment
source venv/bin/activate

# Run the monitor
python yeti.py
```

## Dependencies

- `websockets` - WebSocket client for Jetstream connection
- `requests` - HTTP client for Google Translate API

Install with: `pip install websockets requests`

## Architecture

The application is a single-file async Python script (`yeti.py`) with these main components:

### Core Classes
- **Statistics** - Tracks session metrics (post counts, language distribution, keyword matches)
- **LogFiles** - Manages four log files: `full.log`, `posts.log`, `translated.log`, `URLs.log`

### Key Functions
- **`monitor_jetstream()`** - Main async loop connecting to `wss://jetstream2.us-east.bsky.network/subscribe`
- **`display_post()`** - Processes and displays filtered posts with translations
- **`translate_to_english()`** - Uses Google Translate unofficial API (`translate.googleapis.com`)
- **`is_english()`** - Language detection via language tags and common word patterns

### Data Flow
1. WebSocket receives all posts from Bluesky firehose
2. Posts filtered by keyword match (case-insensitive)
3. Non-English posts translated via Google Translate
4. Results displayed with color coding and logged to files

### Run Modes
- Continuous (until Q pressed)
- Time-limited (hours/minutes/seconds)
- Post-limited (stop after N filtered posts)

### Display Modes
- Original + English translation
- English only (translated content)
- Background mode (live statistics dashboard)

## Log Files

Generated files are gitignored:
- `full.log` - All posts from stream
- `posts.log` - Keyword-matched posts
- `translated.log` - Translations with originals
- `URLs.log` - External links only
- `report.txt` - Session statistics
- `keywords.txt` - Saved keywords
- `*_logs/` - Backup directories
