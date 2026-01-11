# Yeti

Real-time Bluesky post monitor with keyword filtering and automatic Finnish translation.

## Overview

Yeti connects to the Bluesky Jetstream WebSocket service to monitor the global firehose of posts in real-time. It filters posts by user-specified keywords and automatically translates non-Finnish content to Finnish using Google Translate.

## Features

### Core Features
- **Real-time monitoring** - Connects to Bluesky Jetstream WebSocket for live post streaming
- **Multiple keyword filtering** - Monitor multiple keywords simultaneously (case-insensitive)
- **Automatic translation** - Non-Finnish posts are automatically translated to Finnish
- **Language detection** - Detects Finnish using language tags and linguistic pattern analysis
- **Keyword persistence** - Keywords saved to `keywords.txt` for reuse

### Run Modes
- **Continuous** - Run until manually interrupted (Q key or Ctrl+C)
- **Time-limited** - Run for a specific duration (hours/minutes/seconds)
- **Post-limited** - Run until a specific number of filtered posts are collected

### Display Modes
- **Original + Translation** - Show original post with Finnish translation
- **Finnish only** - Show only Finnish content (translations in white, originals hidden)
- **Background mode** - Live statistics dashboard updated every second

### Logging
All data is logged to separate files:
- `full.log` - All posts from Jetstream stream
- `posts.log` - Filtered posts matching keywords
- `translated.log` - Translated posts (original + Finnish)
- `URLs.log` - External URLs only
- `report.txt` - Session statistics report

Existing log files are automatically backed up to timestamped directories (e.g., `20260111_143052_logs/`).

### Additional Features
- **External link extraction** - Displays external links in bright red
- **Internal link filtering** - Hides Bluesky internal links (bsky.app, bsky.social, etc.)
- **Local timezone** - Timestamps converted to local time
- **Auto-reconnect** - Automatically reconnects if connection drops
- **WebSocket keepalive** - Ping every 30 seconds to maintain connection
- **Color-coded output** - Easy-to-read terminal output

## Requirements

- Python 3.7+
- `websockets` - WebSocket client library
- `requests` - HTTP library for translation API

## Installation

1. Clone the repository:
```bash
git clone https://github.com/jonezian/yeti.git
cd yeti
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install websockets requests
```

## Usage

```bash
python yeti.py
```

### Interactive Setup

#### 1. Keywords
```
Enter keywords to monitor (empty line to finish):
  Keyword 1: python
  Keyword 2: javascript
  Keyword 3:

Keywords: python, javascript
```

If `keywords.txt` exists, you'll be asked to reuse saved keywords:
```
Saved keywords found:
  1. python
  2. javascript

Use saved keywords? [Y/n]:
```

#### 2. Run Mode
```
Run mode:
  1 - Run until interrupted (default)
  2 - Run for specific duration
  3 - Run until specific number of filtered posts

Select run mode [1]:
```

#### 3. Display Mode
```
Display mode:
  1 - Show original + Finnish translation (default)
  2 - Show only Finnish (translated)
  3 - Background mode (live statistics only)

Select mode [1]:
```

### Controls

- **Q** - Quit and show report
- **Ctrl+C** - Force quit

### Example Output

#### Normal Mode
```
────────────────────────────────────────────────────────
[14:32:15]

Just learned about Python decorators, they're amazing!

[FI] Opin juuri Python-koristelijoista, ne ovat hämmästyttäviä!

https://example.com/python-tutorial
```

#### Background Mode (Live Statistics)
```
╔══════════════════════════════════════════════════════════╗
║            LIVE STATISTICS  (Press Q to quit)            ║
╚══════════════════════════════════════════════════════════╝

Running time: 00:05:23

Posts:
  Total from stream: 15,432
  Filtered matches:  47
  Match rate:        0.3046%

Keyword matches:
  python: 23
  javascript: 18

Top 10 Languages:
  English: 9,234 (59.8%)
  Japanese: 2,432 (15.8%)
  Portuguese: 1,765 (11.4%)
  ...
```

#### Session Report
```
============================================================
                    SESSION REPORT
============================================================

Time:
  Started:  2026-01-11 14:32:15
  Ended:    2026-01-11 15:45:30
  Duration: 01:13:15

Posts:
  Total from stream: 125,432
  Displayed:         847
  Match rate:        0.68%

Keyword matches:
  python: 423
  javascript: 312

Languages:
  English: 78,234 (62.4%)
  Japanese: 15,432 (12.3%)
  Portuguese: 8,765 (7.0%)
  ...

============================================================
```

## How It Works

### Jetstream Connection

Yeti connects to `wss://jetstream2.us-east.bsky.network/subscribe` with the `wantedCollections=app.bsky.feed.post` parameter to receive only post events from the Bluesky network. WebSocket pings every 30 seconds keep the connection alive.

### Language Detection

Finnish is detected using:
1. **Language tags** - Posts with `fi` language tag are recognized as Finnish
2. **Linguistic patterns**:
   - Finnish special characters (ä, ö, å)
   - Finnish double vowels (aa, ee, ii, oo, uu, yy, ää, öö)
   - Common Finnish words

### Translation

Non-Finnish posts are translated using Google Translate's unofficial API endpoint. Translations that match the original text are filtered out.

### Link Handling

- External links are extracted from post facets and embeds
- Bluesky internal links (bsky.app, bsky.social, blueskyweb.xyz, atproto.com) are hidden
- Only external URLs are displayed and logged

## File Structure

```
yeti/
├── yeti.py           # Main application
├── keywords.txt      # Saved keywords (auto-generated)
├── full.log          # All posts from stream
├── posts.log         # Filtered posts
├── translated.log    # Translations
├── URLs.log          # External links
├── report.txt        # Session report
└── YYYYMMDD_HHMMSS_logs/  # Backup directories
```

## License

MIT License

## Author

Built with Claude Code
