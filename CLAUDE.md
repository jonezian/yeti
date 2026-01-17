# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Yeti is a real-time Bluesky post monitor that connects to the Jetstream WebSocket service. It filters posts by keywords, collects comprehensive statistics, and batch-translates all filtered content to English and Finnish after monitoring ends.

## Running the Application

```bash
# Activate virtual environment
source venv/bin/activate

# Run the monitor
python yeti.py
```

## Dependencies

- `websockets` - WebSocket client for Jetstream connection
- `requests` - HTTP client for Google Translate API and Bluesky profile API
- `concurrent.futures` - Parallel processing (standard library)

Install with: `pip install websockets requests`

## Architecture

The application is a single-file async Python script (`yeti.py`, ~1250 lines) with these main components:

### Constants and Configuration

```python
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
BLUESKY_DOMAINS = frozenset([...])  # O(1) lookup for internal link detection
ENGLISH_WORDS = frozenset([...])    # O(1) lookup for language detection
LANGUAGE_NAMES = {...}               # Language code to full name mapping
LOG_PATTERNS = [...]                 # Patterns for log file backup/archive
```

### Core Classes

#### Statistics (lines 101-373)
Tracks comprehensive session metrics:
- Total and filtered post counts
- Language distribution with percentage breakdown
- Keyword match counts
- Hashtag counts (both stream-wide and filtered)
- Unique profile tracking with post counts per author
- External URL counts

Key methods:
- `record_post()` - Count stream posts
- `record_displayed()` - Track filtered posts and keyword matches
- `record_language()` - Track language distribution
- `record_hashtags()` - Track hashtag usage
- `record_profile()` - Track unique authors with display names
- `print_report()` - Generate colored terminal output and log file

#### LogFiles (lines 376-487)
Manages eight log files with automatic backup of previous sessions:
- `ALL.{timestamp}.log` - All posts from stream (raw text)
- `FILTER.{timestamp}.log` - Keyword-matched posts with metadata
- `ENG_{timestamp}.log` - English translations with author info
- `FIN_{timestamp}.log` - Finnish translations with author info
- `EXT-URL.{timestamp}.log` - External URLs only
- `PROFILES.{timestamp}.log` - Unique profiles with display names
- `FILTER_hashtag.{timestamp}.log` - Hashtags from filtered posts
- `ALL_hashtag.{timestamp}.log` - All hashtags from stream
- `REPORT_{timestamp}.log` - Session statistics report

### Key Functions

#### HTTP Session Management (lines 91-97)
```python
def get_http_session():
    """Reusable HTTP session for API calls (connection pooling)."""
```

#### Profile Fetching (lines 488-530)
```python
def fetch_bluesky_profile(did):
    """Fetch display name and handle from Bluesky API."""

def fetch_profiles_batch(dids, progress_callback=None, max_workers=10):
    """Batch fetch profiles using concurrent requests (ThreadPoolExecutor)."""
```

#### Translation (lines 536-563)
```python
def translate_text(text, target_lang):
    """Unified translation via Google Translate API.
    Returns (translation, source_language_code)."""

def translate_to_english(text):
    """Convenience wrapper for English translation."""

def translate_to_finnish(text):
    """Convenience wrapper for Finnish translation."""
```

#### Batch Translation (lines 636-770)
```python
def batch_translate_posts(skip_countdown=False):
    """Translate all filtered posts to both English and Finnish.
    Features:
    - 10-second countdown with Q to cancel (skipped in auto-archive mode)
    - Parallel profile fetching with ThreadPoolExecutor (10 workers)
    - Parallel translation processing with ThreadPoolExecutor (8 workers)
    - PROFILES log update with display names
    - Progress display during translation
    - Statistics for skipped (already in target language) posts
    """
```

#### Post Processing (lines 782-895)
```python
def display_post(post_data, keywords, keywords_lower, silent_mode=False):
    """Process incoming post:
    - Extract text, timestamp, language, hashtags, external links
    - Check keyword matches (using pre-computed lowercase)
    - Store for batch translation
    - Update statistics
    - Log to appropriate files
    - Display if not in silent mode
    """
```

#### Live Statistics Display (lines 898-975)
```python
def display_live_stats():
    """Display live statistics dashboard including:
    - Running time
    - Stream stats (total posts, posts/second, hashtags)
    - Filter stats (matched posts, match rate, unique profiles)
    - Top 5 keyword matches
    - Top 5 authors with display names and profile links
    - Top 5 languages with percentages
    - Top 5 hashtags
    """
```

#### WebSocket Monitoring (lines 1017-1066)
```python
async def monitor_jetstream(keywords, keywords_lower, silent_mode=False):
    """Main monitoring loop:
    - Connects to Jetstream WebSocket
    - Handles reconnection on disconnect
    - Processes incoming posts
    - Checks run limits (time/post count)
    """
```

### Data Flow

1. **Startup**: User enters keywords (or loads from `keywords.txt`)
2. **Configuration**: User selects run mode and display mode
3. **Monitoring**: WebSocket receives all Bluesky posts
4. **Filtering**: Posts checked against keywords (case-insensitive, pre-computed lowercase)
5. **Storage**: Filtered posts stored in memory for batch translation
6. **Statistics**: Real-time tracking of all metrics
7. **Logging**: Continuous logging to 8 separate files
8. **Shutdown**: User presses Q or limit reached (time-limited mode auto-archives and continues)
9. **Translation**: Batch translate all filtered posts (parallel processing, countdown skipped in auto-archive)
10. **Report**: Generate comprehensive statistics report

### Run Modes

1. **Continuous** - Run until Q pressed (default)
2. **Time-limited** - Run for specified duration (hours/minutes/seconds), auto-archives and continues
3. **Post-limited** - Stop after N filtered posts collected

### Display Modes

1. **Show filtered posts** - Display posts with language, keywords, external links
2. **Background mode** (default) - Live statistics dashboard only

### Optimization Features

- **frozenset for constants**: O(1) lookup for domain and word checks
- **Pre-computed lowercase keywords**: Avoid repeated `.lower()` calls
- **HTTP session reuse**: Connection pooling via `requests.Session()`
- **Unified translation function**: Single `translate_text()` for both languages
- **Helper methods in classes**: Reduced code duplication in Statistics and LogFiles
- **Parallel profile fetching**: `ThreadPoolExecutor` with 10 workers for concurrent API calls
- **Parallel translation**: `ThreadPoolExecutor` with 8 workers for concurrent EN/FI translations
- **Auto-archive optimization**: Skip countdown in time-limited mode for faster cycling

## Log Files

Generated files are gitignored:
- `ALL.*.log` - All posts from stream
- `FILTER.*.log` - Keyword-matched posts with full details
- `ENG_*.log` - English translations with author, timestamp, source language
- `FIN_*.log` - Finnish translations with author, timestamp, source language
- `EXT-URL.*.log` - External URLs extracted from posts
- `PROFILES.*.log` - Unique author profiles with display names
- `FILTER_hashtag.*.log` - Hashtags from filtered posts
- `ALL_hashtag.*.log` - All hashtags from stream
- `REPORT_*.log` - Session statistics report
- `keywords.txt` - Saved keywords for reuse
- `*_logs/` - Backup directories for previous sessions

## API Endpoints Used

### Bluesky Jetstream
- `wss://jetstream2.us-east.bsky.network/subscribe` - Real-time post firehose

### Bluesky Actor API
- `https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile` - Fetch user profile info

### Google Translate (unofficial)
- `https://translate.googleapis.com/translate_a/single` - Translation with auto language detection
