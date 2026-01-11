# Yeti

Real-time Bluesky post monitor with keyword filtering and automatic Finnish translation.

## Overview

Yeti connects to the Bluesky Jetstream WebSocket service to monitor the global firehose of posts in real-time. It filters posts by user-specified keywords and automatically translates non-Finnish content to Finnish using Google Translate.

## Features

- **Real-time monitoring** - Connects to Bluesky Jetstream WebSocket for live post streaming
- **Keyword filtering** - Only displays posts containing your specified keyword (case-insensitive)
- **Automatic translation** - Non-Finnish posts are automatically translated to Finnish
- **Language detection** - Detects Finnish using language tags and linguistic pattern analysis
- **Two display modes**:
  - Show original post + Finnish translation
  - Show only Finnish content (translations displayed, originals hidden)
- **External link extraction** - Displays external links from posts in bright red
- **Internal link filtering** - Hides Bluesky internal links (bsky.app, bsky.social, etc.)
- **Local timezone** - Timestamps are converted to your local time
- **Auto-reconnect** - Automatically reconnects if connection drops
- **Color-coded output**:
  - Bright white: Original posts / Finnish-only mode text
  - Bright light blue: Finnish translations
  - Bright yellow: Timestamps
  - Bright red: External links
  - Bright cyan: Separators and UI elements

## Requirements

- Python 3.7+
- `websockets` - WebSocket client library
- `requests` - HTTP library for translation API

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
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

### Interactive Prompts

1. **Enter keyword**: Type the keyword you want to monitor (e.g., "python", "news", "AI")

2. **Select display mode**:
   - `1` (default): Show original post + Finnish translation
   - `2`: Show only Finnish content (hide originals, show translations)

3. Press `Ctrl+C` to stop monitoring

### Example Session

```
╔════════════════════════════════════════╗
║  Bluesky Jetstream Keyword Monitor     ║
╚════════════════════════════════════════╝

Enter keyword to monitor: python

Display mode:
  1 - Show original + Finnish translation (default)
  2 - Show only Finnish (translated)

Select mode [1]: 1

Connecting to Bluesky Jetstream...
Monitoring for keyword: 'python'
Press Ctrl+C to stop

Connected! Waiting for posts...

────────────────────────────────────────────────────────
[14:32:15]

Just learned about Python decorators, they're amazing!

[FI] Opin juuri Python-koristelijoista, ne ovat hämmästyttäviä!

https://example.com/python-tutorial
```

## How It Works

### Jetstream Connection

Yeti connects to `wss://jetstream2.us-east.bsky.network/subscribe` with the `wantedCollections=app.bsky.feed.post` parameter to receive only post events from the Bluesky network.

### Language Detection

Finnish is detected using:
1. **Language tags** - Posts with `fi` language tag are recognized as Finnish
2. **Linguistic patterns**:
   - Finnish special characters (ä, ö, å)
   - Finnish double vowels (aa, ee, ii, oo, uu, yy, ää, öö)
   - Common Finnish words

### Translation

Non-Finnish posts are translated using Google Translate's unofficial API endpoint. Translations that match the original text (indicating no translation was needed) are filtered out.

### Link Handling

- External links are extracted from post facets and embeds
- Bluesky internal links (bsky.app, bsky.social, blueskyweb.xyz, atproto.com) are hidden
- Only external URLs are displayed

## Architecture

```
yeti.py
├── translate_to_finnish()  - Google Translate API wrapper
├── is_finnish()            - Language detection with pattern matching
├── is_bluesky_link()       - Filter internal Bluesky URLs
├── display_post()          - Format and display matching posts
├── monitor_jetstream()     - WebSocket connection and message handling
└── main()                  - Entry point with user prompts
```

## License

MIT License

## Author

Built with Claude Code
