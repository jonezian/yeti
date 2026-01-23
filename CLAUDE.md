# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Yeti is a real-time Bluesky post monitor that connects to the Jetstream WebSocket service. It filters posts by keywords, collects comprehensive statistics, and supports real-time Finnish translation. Supports both Linux and Windows platforms.

## Running the Application

```bash
# Activate virtual environment
source venv/bin/activate  # Linux/macOS
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run the monitor
python yeti.py

# Run with a log file
python yeti.py -f /path/to/logfile.log

# Run tests
python -m pytest test_yeti.py -v
```

## Dependencies

See `requirements.txt`:
- `websockets>=12.0` - WebSocket client for Jetstream connection
- `requests>=2.31.0` - HTTP client for Google Translate API and Bluesky profile API
- `rich>=13.7.0` - Terminal UI with live updates and tables
- `urllib3>=2.0.0` - HTTP library with retry support

## Architecture

The application is a single-file async Python script (`yeti.py`, ~2250 lines) with these main components:

### Platform Support

```python
IS_WINDOWS = platform.system() == 'Windows'

# Platform-agnostic functions:
check_key_pressed()   # Non-blocking keyboard input
setup_terminal()      # Configure terminal for raw input
restore_terminal()    # Restore terminal settings
```

### Security Constants

```python
MAX_TEXT_LENGTH = 5000   # Max characters for translation
MAX_JSON_SIZE = 1024 * 1024  # 1MB max JSON size
MAX_LINE_LENGTH = 100000  # Max line length when reading files
```

### Precompiled Patterns

```python
HASHTAG_PATTERN = re.compile(r'#(\w+)')
URL_PATTERN = re.compile(r'https?://[^\s]+')
DATE_LOG_PATTERN = re.compile(r'^(\d{4}-\d{2}-\d{2})\.log$')
```

### Validation Functions

```python
def sanitize_text(text: str, max_length: int) -> str:
    """Sanitize and truncate text to max length."""

def validate_url(url: str) -> bool:
    """Validate URL using urlparse (http/https only)."""

def safe_path(path: str) -> str | None:
    """Safely resolve and validate file paths."""

def safe_json_loads(data: str, max_size: int) -> dict | None:
    """Safely parse JSON with size limit."""
```

### Core Classes

#### Statistics
Tracks comprehensive session metrics:
- Total and filtered post counts
- Language distribution with percentage breakdown
- Keyword match counts
- Hashtag counts (both stream-wide and filtered)
- External URL counts

Key methods:
- `record_post()` - Count stream posts
- `record_displayed()` - Track filtered posts and keyword matches
- `record_language()` - Track language distribution
- `record_hashtags()` - Track hashtag usage
- `get_top_keywords(n)` - Get top N keywords using heapq
- `get_top_languages(n)` - Get top N languages using heapq
- `get_top_hashtags(n)` - Get top N hashtags using heapq
- `print_report()` - Generate colored terminal output

#### LogFiles
Manages log files in the `LOGS/` directory:
- `LIVE_{date}.log` - Live stream posts (append mode)
- `FULL_{date}.log` - File analysis results (append mode)

### Key Functions

#### HTTP Session Management
```python
def get_http_session():
    """Reusable HTTP session with retry logic and connection pooling.
    - 3 retries with exponential backoff
    - Pool of 20 connections
    - Handles 429, 500, 502, 503, 504 status codes
    """
```

#### Translation
```python
def translate_to_finnish(text: str) -> tuple[str, str]:
    """Translate text to Finnish using Google Translate API.
    - Input sanitized to MAX_TEXT_LENGTH
    - Returns (translated_text, source_language_code)
    """
```

#### Post Processing
```python
def display_post(post_data: dict, keywords: list[str], keywords_lower: list[str],
                 silent_mode: bool = False, analyzed_mode: bool = False) -> None:
    """Process incoming post:
    - Extract text, timestamp, language, hashtags, external links
    - Check keyword matches (using pre-computed lowercase)
    - Update statistics
    - Log to files
    - Display if not in silent mode
    """
```

#### WebSocket Monitoring
```python
async def monitor_jetstream(keywords: list[str], keywords_lower: list[str],
                           silent_mode: bool = False, analyzed_mode: bool = False) -> None:
    """Main monitoring loop:
    - Connects to Jetstream WebSocket
    - Handles reconnection on disconnect
    - Processes incoming posts
    - Checks run limits (time/post count)
    """
```

### Data Flow

1. **Startup**: User enters keywords (or loads from `keywords.txt`)
2. **Data Source**: Choose between live Jetstream or log file analysis
3. **Configuration**: User selects run mode and display mode
4. **Monitoring**: WebSocket receives all Bluesky posts (or reads from file)
5. **Filtering**: Posts checked against keywords (case-insensitive)
6. **Statistics**: Real-time tracking of all metrics
7. **Logging**: Continuous logging to date-based files
8. **Shutdown**: User presses Q or limit reached
9. **Report**: Generate comprehensive statistics report

### Run Modes

1. **Continuous** - Run until Q pressed (default)
2. **Time-limited** - Run for specified duration, auto-archives and continues
3. **Post-limited** - Stop after N filtered posts collected

### Display Modes

1. **Show filtered posts** - Display posts with language, keywords, external links
2. **Background mode** (default) - Live statistics dashboard only
3. **Translated mode** - Posts translated to Finnish with full details

### Optimization Features

- **Precompiled regex**: Patterns compiled at module level
- **heapq for top-N**: O(n log k) instead of O(n log n) for rankings
- **Pre-computed lowercase keywords**: Avoid repeated `.lower()` calls
- **HTTP session reuse**: Connection pooling with retry logic
- **Platform-agnostic code**: Works on Linux and Windows

## Environment Variables

- `YETI_LOG_DIR` - Additional directory to search for log files

## Log Files

Generated files are gitignored:
- `LOGS/LIVE_{date}.log` - Live stream filtered posts
- `LOGS/FULL_{date}.log` - File analysis results
- `keywords.txt` - Saved keywords for reuse

## Testing

Run the test suite:
```bash
python -m pytest test_yeti.py -v
```

Test coverage includes:
- `format_time()` / `format_number()` - Formatting functions
- `sanitize_text()` / `validate_url()` - Input validation
- `safe_json_loads()` / `safe_path()` - Security functions
- `extract_hashtags()` / `extract_links()` - Data extraction
- `Statistics` class - Metrics tracking

## API Endpoints Used

### Bluesky Jetstream
- `wss://jetstream2.us-east.bsky.network/subscribe` - Real-time post firehose

### Bluesky Actor API
- `https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile` - Fetch user profile info

### Google Translate (unofficial)
- `https://translate.googleapis.com/translate_a/single` - Translation with auto language detection
