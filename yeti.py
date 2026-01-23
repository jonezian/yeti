#!/usr/bin/env python3
"""
Yeti - Bluesky Jetstream Monitor
Real-time post monitoring with keyword filtering.
Connects to Bluesky Jetstream WebSocket service and displays posts matching keywords.
"""

import json
import asyncio
import argparse
import heapq
import logging
import os
import platform
import re
import sys
import threading
import queue
from datetime import datetime
from collections import defaultdict
from urllib.parse import quote_plus, urlparse

# Platform detection
IS_WINDOWS = platform.system() == 'Windows'

# Platform-specific imports
if IS_WINDOWS:
    import msvcrt
else:
    import select
    import termios
    import tty

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('yeti')

import websockets
import requests
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn

# ANSI color codes (for non-rich output)
RESET = "\033[0m"
BRIGHT_CYAN = "\033[1;36m"
BRIGHT_YELLOW = "\033[1;93m"
BRIGHT_WHITE = "\033[1;97m"
BRIGHT_RED = "\033[1;91m"
BRIGHT_GREEN = "\033[1;92m"
BRIGHT_MAGENTA = "\033[1;95m"
BRIGHT_LIGHT_BLUE = "\033[1;96m"

# Rich console
console = Console()


def format_time(seconds: float) -> str:
    """Format seconds as human-readable time."""
    if seconds < 0:
        return "—"
    elif seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours}h {mins}m {secs}s"


def format_number(n: int) -> str:
    """Format number with K/M suffix for large numbers."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 10_000:
        return f"{n/1_000:.1f}K"
    else:
        return f"{n:,}"

# Jetstream WebSocket endpoint
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

# Language code to full name mapping
LANGUAGE_NAMES = {
    'af': 'Afrikaans', 'am': 'Amharic', 'ar': 'Arabic', 'az': 'Azerbaijani',
    'be': 'Belarusian', 'bg': 'Bulgarian', 'bn': 'Bengali', 'bs': 'Bosnian',
    'ca': 'Catalan', 'cs': 'Czech', 'cy': 'Welsh', 'da': 'Danish',
    'de': 'German', 'el': 'Greek', 'en': 'English', 'eo': 'Esperanto',
    'es': 'Spanish', 'et': 'Estonian', 'eu': 'Basque', 'fa': 'Persian',
    'fi': 'Finnish', 'fr': 'French', 'ga': 'Irish', 'gl': 'Galician',
    'gu': 'Gujarati', 'he': 'Hebrew', 'hi': 'Hindi', 'hr': 'Croatian',
    'hu': 'Hungarian', 'hy': 'Armenian', 'id': 'Indonesian', 'is': 'Icelandic',
    'it': 'Italian', 'ja': 'Japanese', 'ka': 'Georgian', 'kk': 'Kazakh',
    'km': 'Khmer', 'kn': 'Kannada', 'ko': 'Korean', 'ku': 'Kurdish',
    'ky': 'Kyrgyz', 'la': 'Latin', 'lo': 'Lao', 'lt': 'Lithuanian',
    'lv': 'Latvian', 'mk': 'Macedonian', 'ml': 'Malayalam', 'mn': 'Mongolian',
    'mr': 'Marathi', 'ms': 'Malay', 'mt': 'Maltese', 'my': 'Burmese',
    'ne': 'Nepali', 'nl': 'Dutch', 'no': 'Norwegian', 'pa': 'Punjabi',
    'pl': 'Polish', 'ps': 'Pashto', 'pt': 'Portuguese', 'ro': 'Romanian',
    'ru': 'Russian', 'sd': 'Sindhi', 'si': 'Sinhala', 'sk': 'Slovak',
    'sl': 'Slovenian', 'so': 'Somali', 'sq': 'Albanian', 'sr': 'Serbian',
    'sv': 'Swedish', 'sw': 'Swahili', 'ta': 'Tamil', 'te': 'Telugu',
    'tg': 'Tajik', 'th': 'Thai', 'tl': 'Tagalog', 'tr': 'Turkish',
    'uk': 'Ukrainian', 'ur': 'Urdu', 'uz': 'Uzbek', 'vi': 'Vietnamese',
    'yi': 'Yiddish', 'zh': 'Chinese', 'zu': 'Zulu', 'unknown': 'Unknown',
}

# Global state
stats = None
quit_flag = False
graceful_stop = False  # S key - finish current work then stop
log_files = None
run_time_limit = None
run_post_limit = None
auto_restart = True  # Whether to auto-restart after time limit
data_source = None  # 'jetstream' or file path

# Queue for analyzed mode
post_queue = queue.Queue()
queue_processing = False
current_processing_post = None


# ═══════════════════════════════════════════════════════════════════════════════
# Platform-agnostic Terminal Functions
# ═══════════════════════════════════════════════════════════════════════════════

def check_key_pressed() -> str | None:
    """Check if a key is pressed without blocking.

    Returns:
        The key pressed (lowercase) or None if no key pressed.
    """
    if IS_WINDOWS:
        if msvcrt.kbhit():
            try:
                key = msvcrt.getch()
                # Handle special keys (arrows, function keys)
                if key in (b'\x00', b'\xe0'):
                    msvcrt.getch()  # Consume the second byte
                    return None
                return key.decode('utf-8', errors='ignore').lower()
            except Exception:
                return None
        return None
    else:
        ready, _, _ = select.select([sys.stdin], [], [], 0.05)
        if ready:
            try:
                return sys.stdin.read(1).lower()
            except Exception:
                return None
        return None


def setup_terminal():
    """Setup terminal for raw keyboard input.

    Returns:
        Previous terminal settings (for restore_terminal) or None on Windows.
    """
    if IS_WINDOWS:
        return None  # Windows doesn't need special setup
    else:
        try:
            old_settings = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
            return old_settings
        except Exception:
            return None


def restore_terminal(old_settings):
    """Restore terminal to previous settings.

    Args:
        old_settings: Settings returned by setup_terminal()
    """
    if old_settings is not None and not IS_WINDOWS:
        try:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except Exception:
            pass




class Statistics:
    """Track statistics for the monitoring session."""

    def __init__(self, keywords):
        self.keywords = keywords
        self.keywords_lower = [kw.lower() for kw in keywords]  # Pre-compute lowercase
        self.start_time = datetime.now()
        self.end_time = None
        self.total_posts = 0
        self.displayed_posts = 0
        self.keyword_counts = defaultdict(int)
        self.language_counts = defaultdict(int)
        self.hashtag_counts = defaultdict(int)
        self.all_hashtag_counts = defaultdict(int)
        self.total_urls = 0

    def record_post(self):
        """Record a post from the stream."""
        self.total_posts += 1

    def record_language(self, langs):
        """Record language from a filtered post."""
        if langs:
            for lang in langs:
                self.language_counts[lang] += 1
        else:
            self.language_counts['unknown'] += 1

    def record_displayed(self, text_lower, keywords):
        """Record a displayed post and which keywords matched."""
        self.displayed_posts += 1
        for kw in keywords:
            if kw.lower() in text_lower:
                self.keyword_counts[kw] += 1

    def record_hashtags(self, hashtags):
        """Record hashtags from a filtered post."""
        for tag in hashtags:
            self.hashtag_counts[tag.lower()] += 1

    def record_all_hashtags(self, hashtags):
        """Record hashtags from any post in stream."""
        for tag in hashtags:
            self.all_hashtag_counts[tag.lower()] += 1

    def record_url(self):
        """Record URL count."""
        self.total_urls += 1

    def finish(self):
        """Mark the session as finished."""
        self.end_time = datetime.now()

    def get_duration(self):
        """Get the duration of the session."""
        return (self.end_time or datetime.now()) - self.start_time

    def get_elapsed_seconds(self):
        """Get elapsed time in seconds."""
        return self.get_duration().total_seconds()

    def posts_per_second(self):
        """Calculate stream posts per second."""
        elapsed = self.get_elapsed_seconds()
        return self.total_posts / elapsed if elapsed > 0 else 0

    def filtered_per_minute(self):
        """Calculate filtered posts per minute."""
        elapsed = self.get_elapsed_seconds()
        return (self.displayed_posts / elapsed * 60) if elapsed > 0 else 0

    def match_rate(self):
        """Calculate match rate percentage."""
        if self.total_posts > 0:
            return (self.displayed_posts / self.total_posts) * 100
        return 0

    def eta_seconds(self, time_limit=None, post_limit=None):
        """Estimate time remaining based on limits."""
        elapsed = self.get_elapsed_seconds()
        if elapsed <= 0:
            return -1

        if time_limit is not None:
            return max(0, time_limit - elapsed)

        if post_limit is not None and self.displayed_posts > 0:
            rate = self.displayed_posts / elapsed
            if rate > 0:
                remaining_posts = post_limit - self.displayed_posts
                return remaining_posts / rate

        return -1

    def estimated_posts_at_end(self, time_limit=None):
        """Estimate total filtered posts when time limit ends."""
        if time_limit is None:
            return -1
        rate = self.filtered_per_minute() / 60  # posts per second
        if rate > 0:
            return int(rate * time_limit)
        return -1

    def get_top_keywords(self, n: int = 5) -> list[tuple[str, int]]:
        """Get top N keywords by count using heapq for efficiency.

        Args:
            n: Number of top items to return

        Returns:
            List of (keyword, count) tuples sorted by count descending
        """
        if not self.keyword_counts:
            return []
        items = [(kw, self.keyword_counts[kw])
                 for kw in self.keywords if self.keyword_counts.get(kw, 0) > 0]
        return heapq.nlargest(n, items, key=lambda x: x[1])

    def get_top_languages(self, n: int = 5) -> list[tuple[str, int]]:
        """Get top N languages by count using heapq for efficiency.

        Args:
            n: Number of top items to return

        Returns:
            List of (language_code, count) tuples sorted by count descending
        """
        if not self.language_counts:
            return []
        return heapq.nlargest(n, self.language_counts.items(), key=lambda x: x[1])

    def get_top_hashtags(self, n: int = 5, filtered: bool = True) -> list[tuple[str, int]]:
        """Get top N hashtags by count using heapq for efficiency.

        Args:
            n: Number of top items to return
            filtered: If True, use filtered hashtags; if False, use all stream hashtags

        Returns:
            List of (hashtag, count) tuples sorted by count descending
        """
        counts = self.hashtag_counts if filtered else self.all_hashtag_counts
        if not counts:
            return []
        return heapq.nlargest(n, counts.items(), key=lambda x: x[1])

    def print_report(self):
        """Print the statistics report to terminal."""
        duration = self.get_duration()
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed = duration.total_seconds()

        posts_per_sec = self.total_posts / elapsed if elapsed > 0 else 0
        filtered_per_min = (self.displayed_posts / elapsed * 60) if elapsed > 0 else 0

        # Build text report
        lines = [
            "=" * 70,
            "                         SESSION REPORT",
            "=" * 70, "",
            "TIME:",
            f"  Started:  {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Ended:    {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Duration: {hours:02d}:{minutes:02d}:{seconds:02d}", "",
            "STREAM STATISTICS:",
            f"  Total posts processed:    {self.total_posts:,}",
            f"  Average posts/second:     {posts_per_sec:.1f}",
            f"  Total hashtags (stream):  {sum(self.all_hashtag_counts.values()):,}",
            f"  Unique hashtags (stream): {len(self.all_hashtag_counts):,}", "",
            "FILTERED STATISTICS:",
            f"  Filtered posts:           {self.displayed_posts:,}",
        ]

        if self.total_posts > 0:
            pct = (self.displayed_posts / self.total_posts) * 100
            lines.append(f"  Match rate:               {pct:.4f}%")

        lines.extend([
            f"  Filtered posts/minute:    {filtered_per_min:.1f}",
            f"  External URLs:            {self.total_urls:,}",
            f"  Hashtags (filtered):      {sum(self.hashtag_counts.values()):,}",
            f"  Unique hashtags (filter): {len(self.hashtag_counts):,}", "",
        ])

        # Top 5 sections
        self._add_top_section(lines, "TOP 5 KEYWORD MATCHES:", self.keyword_counts, self.keywords)
        self._add_top_langs(lines)
        self._add_top_hashtags(lines, "TOP 5 HASHTAGS (filtered):", self.hashtag_counts)
        self._add_top_hashtags(lines, "TOP 5 HASHTAGS (all stream):", self.all_hashtag_counts)

        lines.extend(["", "=" * 70])

        # Print colored version
        self._print_colored_report(hours, minutes, seconds, posts_per_sec, filtered_per_min)

    def _add_top_section(self, lines, title, counts, items=None):
        """Add a top 5 section to report lines."""
        lines.append(title)
        if counts:
            if items:
                sorted_items = sorted([i for i in items if counts.get(i, 0) > 0],
                                     key=lambda x: counts.get(x, 0), reverse=True)
            else:
                sorted_items = sorted(counts.keys(), key=lambda x: counts[x], reverse=True)
            for item in sorted_items[:5]:
                lines.append(f"  {item}: {counts[item]:,}")
            if len(sorted_items) > 5:
                lines.append(f"  ... and {len(sorted_items) - 5} more")
        else:
            lines.append("  No data")
        lines.append("")

    def _add_top_langs(self, lines):
        """Add top 5 languages section."""
        lines.append("TOP 5 LANGUAGES (filtered posts):")
        if self.language_counts:
            sorted_langs = sorted(self.language_counts.items(), key=lambda x: x[1], reverse=True)
            for code, count in sorted_langs[:5]:
                pct = (count / self.displayed_posts * 100) if self.displayed_posts > 0 else 0
                name = LANGUAGE_NAMES.get(code, code)
                lines.append(f"  {name}: {count:,} ({pct:.1f}%)")
            if len(sorted_langs) > 5:
                lines.append(f"  ... and {len(sorted_langs) - 5} more languages")
        else:
            lines.append("  No language data")
        lines.append("")

    def _add_top_hashtags(self, lines, title, counts):
        """Add top 5 hashtags section."""
        lines.append(title)
        if counts:
            sorted_tags = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            for tag, count in sorted_tags[:5]:
                lines.append(f"  #{tag}: {count:,}")
            if len(sorted_tags) > 5:
                lines.append(f"  ... and {len(sorted_tags) - 5} more hashtags")
        else:
            lines.append("  No hashtags")
        lines.append("")

    def _print_colored_report(self, hours, minutes, seconds, posts_per_sec, filtered_per_min):
        """Print colored version of report to terminal using Rich."""
        console.print()

        # Time table
        time_table = Table(show_header=True, header_style="bold white", box=None)
        time_table.add_column("⏱️  AIKA", style="cyan", width=25)
        time_table.add_column("", style="white")
        time_table.add_row("Aloitettu:", f"[yellow]{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}[/yellow]")
        time_table.add_row("Lopetettu:", f"[yellow]{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}[/yellow]")
        time_table.add_row("Kesto:", f"[bold yellow]{hours:02d}:{minutes:02d}:{seconds:02d}[/bold yellow]")

        # Stream stats table
        stream_table = Table(show_header=True, header_style="bold white", box=None)
        stream_table.add_column("📡 STREAM-TILASTOT", style="cyan", width=25)
        stream_table.add_column("", style="white")
        stream_table.add_row("Posteja yhteensä:", f"[green]{self.total_posts:,}[/green]")
        stream_table.add_row("Posteja/sekunti:", f"[green]{posts_per_sec:.1f}[/green]")
        stream_table.add_row("Hashtageja yhteensä:", f"[green]{sum(self.all_hashtag_counts.values()):,}[/green]")
        stream_table.add_row("Uniikkeja hashtageja:", f"[green]{len(self.all_hashtag_counts):,}[/green]")

        # Filter stats table
        filter_table = Table(show_header=True, header_style="bold white", box=None)
        filter_table.add_column("🎯 SUODATETUT TILASTOT", style="cyan", width=25)
        filter_table.add_column("", style="white")
        filter_table.add_row("Osumia yhteensä:", f"[green]{self.displayed_posts:,}[/green]")
        if self.total_posts > 0:
            pct = (self.displayed_posts / self.total_posts) * 100
            filter_table.add_row("Osuma-%:", f"[green]{pct:.4f}%[/green]")
        filter_table.add_row("Osumia/minuutti:", f"[green]{filtered_per_min:.1f}[/green]")
        filter_table.add_row("Ulkoisia URL:eja:", f"[green]{self.total_urls:,}[/green]")

        # Build content
        content = Group(
            time_table,
            "",
            stream_table,
            "",
            filter_table,
            "",
            self._get_top_keywords_table(),
            "",
            self._get_top_languages_table(),
            "",
            self._get_top_hashtags_table(),
        )

        console.print(Panel(
            content,
            title="[bold blue]📊 SESSION REPORT[/bold blue]",
            border_style="blue",
            padding=(1, 2)
        ))
        console.print()

    def _get_top_keywords_table(self) -> Table:
        """Get top keywords as Rich table."""
        table = Table(show_header=True, header_style="bold yellow", box=None)
        table.add_column("🔑 TOP AVAINSANAT", style="yellow", width=30)
        table.add_column("Osumia", justify="right", width=10)

        top_kw = self.get_top_keywords(5)
        if top_kw:
            for kw, count in top_kw:
                table.add_row(kw, f"[green]{count:,}[/green]")
            total_with_matches = len([k for k in self.keywords if self.keyword_counts.get(k, 0) > 0])
            if total_with_matches > 5:
                table.add_row(f"[dim]... +{total_with_matches - 5} muuta[/dim]", "")
        else:
            table.add_row("[dim]Ei osumia[/dim]", "")
        return table

    def _get_top_languages_table(self) -> Table:
        """Get top languages as Rich table."""
        table = Table(show_header=True, header_style="bold magenta", box=None)
        table.add_column("🌍 TOP KIELET", style="magenta", width=25)
        table.add_column("Posteja", justify="right", width=10)
        table.add_column("%", justify="right", width=8)

        top_langs = self.get_top_languages(5)
        if top_langs:
            for code, count in top_langs:
                pct = (count / self.displayed_posts * 100) if self.displayed_posts > 0 else 0
                name = LANGUAGE_NAMES.get(code, code)
                table.add_row(name, f"[green]{count:,}[/green]", f"{pct:.1f}%")
            if len(self.language_counts) > 5:
                table.add_row(f"[dim]... +{len(self.language_counts) - 5} muuta[/dim]", "", "")
        else:
            table.add_row("[dim]Ei kielitietoja[/dim]", "", "")
        return table

    def _get_top_hashtags_table(self) -> Table:
        """Get top hashtags as Rich table."""
        table = Table(show_header=True, header_style="bold green", box=None)
        table.add_column("# TOP HASHTAGIT", style="green", width=30)
        table.add_column("Käyttöjä", justify="right", width=10)

        top_tags = self.get_top_hashtags(5, filtered=True)
        if top_tags:
            for tag, count in top_tags:
                table.add_row(f"#{tag}", f"[green]{count:,}[/green]")
            if len(self.hashtag_counts) > 5:
                table.add_row(f"[dim]... +{len(self.hashtag_counts) - 5} muuta[/dim]", "")
        else:
            table.add_row("[dim]Ei hashtageja[/dim]", "")
        return table


class LogFiles:
    """Handle logging to LOGS directory."""

    def __init__(self, source_type='live'):
        """Initialize log files.

        Args:
            source_type: 'live' for Jetstream, 'file' for log file analysis
        """
        self.date_str = datetime.now().strftime('%Y-%m-%d')
        self.source_type = source_type

        # Create LOGS directory if it doesn't exist
        os.makedirs('LOGS', exist_ok=True)

        # File prefix based on source
        prefix = 'LIVE' if source_type == 'live' else 'FULL'
        self.all_filename = f'LOGS/{prefix}_{self.date_str}.log'
        self.all_log = open(self.all_filename, 'a', encoding='utf-8')  # Append mode

    def log_all(self, text, timestamp):
        """Log all posts from stream."""
        self.all_log.write(f"[{timestamp}] {text}\n")

    def flush_all(self):
        """Flush log file."""
        if self.all_log:
            self.all_log.flush()

    def archive(self):
        """Finalize log file."""
        console.print(f"[green]✓ Loki tallennettu: {self.all_filename}[/green]")

    def close(self):
        """Close log file."""
        if self.all_log:
            self.all_log.close()


def cleanup_logs_directory():
    """Cleanup function - currently no-op as we use date-based append files."""
    pass


def prescan_data_file(file_path, keywords, keywords_lower, stats_obj, log_files_obj):
    """Pre-scan a data file to build statistics without live display.

    This builds initial statistics from the data file before the final
    statistics report is shown.

    Args:
        file_path: Path to the JSON log file
        keywords: List of keywords to match
        keywords_lower: Pre-computed lowercase keywords
        stats_obj: Statistics object to update
        log_files_obj: LogFiles object for logging

    Returns: Tuple of (lines_processed, file_end_position)
    """
    if not os.path.exists(file_path):
        return 0, 0

    console.print(f"\n[bold cyan]📊 Esikäsitellään datatiedosto tilastojen rakentamiseksi...[/bold cyan]")
    console.print(f"[dim]Tiedosto: {file_path}[/dim]")

    line_count = 0
    matched_count = 0
    file_end_position = 0

    try:
        # Get file size for progress
        file_size = os.path.getsize(file_path)

        with open(file_path, 'r', encoding='utf-8') as f:
            from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskProgressColumn

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("{task.fields[stats]}"),
                console=console
            ) as progress:
                task = progress.add_task(
                    "[cyan]Käsitellään...",
                    total=file_size,
                    stats=""
                )

                bytes_read = 0

                for line in f:
                    bytes_read += len(line.encode('utf-8'))
                    line = line.strip()
                    if not line:
                        continue

                    # Skip overly long lines
                    if len(line) > MAX_LINE_LENGTH:
                        logger.warning(f"Skipping line exceeding max length ({MAX_LINE_LENGTH})")
                        continue

                    try:
                        data = safe_json_loads(line)
                        if data is None:
                            continue

                        # Process based on format
                        record = None
                        did = ''

                        if data.get('kind') == 'commit':
                            commit = data.get('commit', {})
                            if commit.get('operation') == 'create' and commit.get('collection') == 'app.bsky.feed.post':
                                record = commit.get('record', {})
                                did = data.get('did', '')
                        elif 'commit' in data:
                            commit = data.get('commit', {})
                            record = commit.get('record', {})
                            did = data.get('did', '')
                        elif 'record' in data or 'text' in data:
                            record = data if 'text' in data else data.get('record', data)
                            did = data.get('did', '')

                        if record:
                            text = record.get('text', '')
                            langs = record.get('langs', [])
                            created_at = record.get('createdAt', '')

                            # Parse timestamp
                            try:
                                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00')).astimezone()
                                time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                            except Exception:
                                time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                            # Record post
                            stats_obj.record_post()

                            # Extract and record hashtags
                            hashtags = extract_hashtags(record)
                            if hashtags:
                                stats_obj.record_all_hashtags(hashtags)

                            if text:
                                # Check keyword matches
                                text_lower = text.lower()
                                if '*' in keywords:
                                    matched = True
                                else:
                                    matched = any(kw_low in text_lower for kw_low in keywords_lower)

                                if matched:
                                    matched_count += 1
                                    stats_obj.record_language(langs)
                                    stats_obj.record_displayed(text_lower, keywords)
                                    if hashtags:
                                        stats_obj.record_hashtags(hashtags)

                                    # Extract and count URLs
                                    links = extract_links(record)
                                    for _ in links:
                                        stats_obj.record_url()

                                    # Log filtered post to FULL file
                                    if log_files_obj:
                                        log_files_obj.log_all(text, time_str)

                            line_count += 1

                    except json.JSONDecodeError:
                        continue
                    except Exception:
                        continue

                    # Update progress periodically
                    if line_count % 1000 == 0:
                        progress.update(
                            task,
                            completed=bytes_read,
                            stats=f"[green]{line_count:,} postiä, {matched_count:,} osumaa[/green]"
                        )

                # Final update
                progress.update(task, completed=file_size, stats=f"[green]{line_count:,} postiä, {matched_count:,} osumaa[/green]")

            # Get file position at end
            file_end_position = f.tell()

        console.print(f"[bold green]✓ Esikäsitelty: {line_count:,} postiä, {matched_count:,} osumaa[/bold green]\n")

        # Flush logs
        if log_files_obj:
            log_files_obj.flush_all()

        return line_count, file_end_position

    except Exception as e:
        console.print(f"[bold red]✗ Virhe esikäsittelyssä: {e}[/bold red]")
        return 0, 0


# ═══════════════════════════════════════════════════════════════════════════════
# Constants for Security and Limits
# ═══════════════════════════════════════════════════════════════════════════════

MAX_TEXT_LENGTH = 5000  # Max characters for translation
MAX_JSON_SIZE = 1024 * 1024  # 1MB max JSON size
MAX_LINE_LENGTH = 100000  # Max line length when reading files

# ═══════════════════════════════════════════════════════════════════════════════
# Precompiled Regex Patterns (for performance)
# ═══════════════════════════════════════════════════════════════════════════════

HASHTAG_PATTERN = re.compile(r'#(\w+)')
URL_PATTERN = re.compile(r'https?://[^\s]+')
DATE_LOG_PATTERN = re.compile(r'^(\d{4}-\d{2}-\d{2})\.log$')


# ═══════════════════════════════════════════════════════════════════════════════
# Validation Functions
# ═══════════════════════════════════════════════════════════════════════════════

def sanitize_text(text: str, max_length: int = MAX_TEXT_LENGTH) -> str:
    """Sanitize and truncate text to max length.

    Args:
        text: Input text to sanitize
        max_length: Maximum allowed length

    Returns:
        Sanitized text, truncated if necessary
    """
    if not text:
        return ""
    text = text.strip()
    if len(text) > max_length:
        return text[:max_length]
    return text


def validate_url(url: str) -> bool:
    """Validate URL using urlparse.

    Args:
        url: URL string to validate

    Returns:
        True if URL is valid, False otherwise
    """
    if not url:
        return False
    try:
        result = urlparse(url)
        return all([result.scheme in ('http', 'https'), result.netloc])
    except Exception:
        return False


def safe_path(path: str) -> str | None:
    """Safely resolve and validate a file path.

    Prevents directory traversal and symlink attacks.

    Args:
        path: Path string to validate

    Returns:
        Resolved absolute path or None if invalid
    """
    if not path:
        return None
    try:
        # Resolve to absolute path, following symlinks
        resolved = os.path.realpath(path)
        # Ensure the resolved path exists
        if os.path.exists(resolved):
            return resolved
        return None
    except Exception:
        return None


def safe_json_loads(data: str, max_size: int = MAX_JSON_SIZE) -> dict | None:
    """Safely parse JSON with size limit.

    Args:
        data: JSON string to parse
        max_size: Maximum allowed size in bytes

    Returns:
        Parsed dict or None if invalid/too large
    """
    if not data:
        return None
    if len(data.encode('utf-8', errors='ignore')) > max_size:
        logger.warning(f"JSON data exceeds max size ({max_size} bytes)")
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        logger.debug(f"JSON decode error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Translation and Analysis Functions
# ═══════════════════════════════════════════════════════════════════════════════

def get_http_session():
    """Get or create a reusable HTTP session with retry logic."""
    if not hasattr(get_http_session, '_session'):
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        get_http_session._session = session
    return get_http_session._session


def translate_to_finnish(text: str) -> tuple[str, str]:
    """Translate text to Finnish using Google Translate API.

    Args:
        text: Text to translate (will be truncated if too long)

    Returns:
        Tuple of (translated_text, source_language_code)
    """
    if not text or not text.strip():
        return text, 'unknown'

    # Sanitize input
    text = sanitize_text(text, MAX_TEXT_LENGTH)

    try:
        session = get_http_session()
        url = "https://translate.googleapis.com/translate_a/single"
        params = {
            'client': 'gtx',
            'sl': 'auto',
            'tl': 'fi',
            'dt': 't',
            'q': text
        }

        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            result = response.json()
            translated = ''.join(part[0] for part in result[0] if part[0])
            source_lang = result[2] if len(result) > 2 else 'unknown'
            return translated, source_lang
    except requests.exceptions.Timeout:
        logger.warning("Translation request timed out")
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Translation connection error: {e}")
    except json.JSONDecodeError as e:
        logger.warning(f"Translation JSON decode error: {e}")
    except Exception as e:
        logger.debug(f"Translation error: {e}")

    return text, 'unknown'




# ═══════════════════════════════════════════════════════════════════════════════
# Rich UI Display Functions
# ═══════════════════════════════════════════════════════════════════════════════

def get_status_text() -> Text:
    """Create status text showing current monitoring state."""
    global run_time_limit, run_post_limit, auto_restart, data_source

    status = Text()

    # Data source line
    if data_source and data_source != 'jetstream':
        status.append("📂 ", style="yellow")
        status.append("LOKITIEDOSTO: ", style="bold yellow")
        # Show just filename, not full path
        filename = os.path.basename(data_source)
        status.append(filename, style="yellow")
    else:
        status.append("📡 ", style="green")
        status.append("LIVE JETSTREAM", style="bold green")

    status.append("\n")

    # Run mode line
    if run_time_limit is not None:
        status.append("⏱️  ", style="cyan")
        status.append("Aikarajoitettu monitorointi", style="bold cyan")
        if auto_restart:
            status.append(" (jatkuva sykli)", style="dim")
        else:
            status.append(" (kertaluonteinen)", style="dim")
    elif run_post_limit is not None:
        status.append("📊 ", style="magenta")
        status.append("Postirajoitettu monitorointi", style="bold magenta")
    else:
        status.append("🔄 ", style="green")
        status.append("Jatkuva monitorointi", style="bold green")

    return status


def create_time_stats_table() -> Table:
    """Create table showing time statistics."""
    global stats, run_time_limit, run_post_limit

    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Label", style="cyan", width=20)
    table.add_column("Value", style="white")
    table.add_column("Label2", style="cyan", width=20)
    table.add_column("Value2", style="white")

    elapsed = stats.get_elapsed_seconds()
    duration = stats.get_duration()
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)

    # Time info
    eta = stats.eta_seconds(run_time_limit, run_post_limit)

    table.add_row(
        "⏱️  Kulunut aika:",
        f"[bold yellow]{hours:02d}:{minutes:02d}:{seconds:02d}[/bold yellow]",
        "⏳ Jäljellä:" if eta >= 0 else "",
        f"[bold green]{format_time(eta)}[/bold green]" if eta >= 0 else ""
    )

    # Progress for limited modes
    if run_time_limit is not None:
        progress_pct = min(100, (elapsed / run_time_limit) * 100)
        bar_width = 20
        filled = int(bar_width * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        table.add_row(
            "📈 Edistyminen:",
            f"[cyan]{bar}[/cyan] [bold]{progress_pct:.1f}%[/bold]",
            "🎯 Tavoite:",
            f"[dim]{format_time(run_time_limit)}[/dim]"
        )
    elif run_post_limit is not None:
        progress_pct = min(100, (stats.displayed_posts / run_post_limit) * 100)
        bar_width = 20
        filled = int(bar_width * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        table.add_row(
            "📈 Edistyminen:",
            f"[cyan]{bar}[/cyan] [bold]{progress_pct:.1f}%[/bold]",
            "🎯 Tavoite:",
            f"[dim]{run_post_limit:,} postiä[/dim]"
        )

    return table


def create_stream_stats_table() -> Table:
    """Create table showing stream statistics."""
    global stats

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold blue")
    table.add_column("📡 STREAM", style="cyan", width=22)
    table.add_column("Arvo", style="white", width=15)
    table.add_column("", style="cyan", width=22)
    table.add_column("Arvo", style="white", width=15)

    pps = stats.posts_per_second()
    total_hashtags = sum(stats.all_hashtag_counts.values())

    table.add_row(
        "Posteja yhteensä:",
        f"[bold green]{format_number(stats.total_posts)}[/bold green]",
        "Posteja/sekunti:",
        f"[bold yellow]{pps:.1f}[/bold yellow]"
    )
    table.add_row(
        "Hashtageja yhteensä:",
        f"[green]{format_number(total_hashtags)}[/green]",
        "Uniikkeja hashtageja:",
        f"[green]{format_number(len(stats.all_hashtag_counts))}[/green]"
    )

    return table


def create_filter_stats_table() -> Table:
    """Create table showing filter statistics."""
    global stats, run_time_limit

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold magenta")
    table.add_column("🎯 SUODATETUT", style="cyan", width=22)
    table.add_column("Arvo", style="white", width=15)
    table.add_column("", style="cyan", width=22)
    table.add_column("Arvo", style="white", width=15)

    fpm = stats.filtered_per_minute()
    match_rate = stats.match_rate()
    filter_hashtags = sum(stats.hashtag_counts.values())

    table.add_row(
        "Osumia yhteensä:",
        f"[bold green]{format_number(stats.displayed_posts)}[/bold green]",
        "Osumia/minuutti:",
        f"[bold yellow]{fpm:.1f}[/bold yellow]"
    )
    table.add_row(
        "Osuma-%:",
        f"[green]{match_rate:.4f}%[/green]",
        "Ulkoisia URL:eja:",
        f"[green]{format_number(stats.total_urls)}[/green]"
    )
    table.add_row(
        "Hashtageja:",
        f"[green]{format_number(filter_hashtags)}[/green]",
        "",
        ""
    )

    # Estimate if time-limited
    if run_time_limit is not None:
        estimated = stats.estimated_posts_at_end(run_time_limit)
        if estimated > 0:
            table.add_row(
                "📊 Arvio lopussa:",
                f"[dim italic]~{format_number(estimated)} osumaa[/dim italic]",
                "",
                ""
            )

    return table


def create_top_keywords_table() -> Table:
    """Create table showing top keywords."""
    global stats

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold yellow")
    table.add_column("🔑 TOP AVAINSANAT", style="yellow", width=30)
    table.add_column("Osumia", style="white", justify="right", width=10)
    table.add_column("%", style="dim", justify="right", width=8)

    top_kw = stats.get_top_keywords(5)
    if top_kw:
        for kw, count in top_kw:
            pct = (count / stats.displayed_posts * 100) if stats.displayed_posts > 0 else 0
            table.add_row(kw, f"[bold]{count:,}[/bold]", f"{pct:.1f}%")
        total_with_matches = len([k for k in stats.keywords if stats.keyword_counts.get(k, 0) > 0])
        if total_with_matches > 5:
            table.add_row(f"[dim]... +{total_with_matches - 5} muuta[/dim]", "", "")
    else:
        table.add_row("[dim]Ei osumia vielä[/dim]", "", "")

    return table


def create_top_languages_table() -> Table:
    """Create table showing top languages."""
    global stats

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold magenta")
    table.add_column("🌍 TOP KIELET", style="magenta", width=30)
    table.add_column("Posteja", style="white", justify="right", width=10)
    table.add_column("%", style="dim", justify="right", width=8)

    top_langs = stats.get_top_languages(5)
    if top_langs:
        for code, count in top_langs:
            name = LANGUAGE_NAMES.get(code, code)
            pct = (count / stats.displayed_posts * 100) if stats.displayed_posts > 0 else 0
            table.add_row(name, f"[bold]{count:,}[/bold]", f"{pct:.1f}%")
        if len(stats.language_counts) > 5:
            table.add_row(f"[dim]... +{len(stats.language_counts) - 5} muuta[/dim]", "", "")
    else:
        table.add_row("[dim]Ei kielitietoja vielä[/dim]", "", "")

    return table


def create_top_hashtags_table() -> Table:
    """Create table showing top hashtags."""
    global stats

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold green")
    table.add_column("# TOP HASHTAGIT", style="green", width=30)
    table.add_column("Käyttöjä", style="white", justify="right", width=10)
    table.add_column("%", style="dim", justify="right", width=8)

    top_tags = stats.get_top_hashtags(5, filtered=True)
    if top_tags:
        for tag, count in top_tags:
            pct = (count / stats.displayed_posts * 100) if stats.displayed_posts > 0 else 0
            table.add_row(f"#{tag}", f"[bold]{count:,}[/bold]", f"{pct:.1f}%")
        if len(stats.hashtag_counts) > 5:
            table.add_row(f"[dim]... +{len(stats.hashtag_counts) - 5} muuta[/dim]", "", "")
    else:
        table.add_row("[dim]Ei hashtageja vielä[/dim]", "", "")

    return table


def create_display() -> Panel:
    """Create the full rich display panel."""
    global stats

    if not stats:
        return Panel("Alustetaan...", title="[bold blue]❄️ Yeti Monitor[/bold blue]")

    # Keywords display
    keywords_text = Text()
    keywords_text.append("🔍 Avainsanat: ", style="bold white")
    keywords_text.append(", ".join(stats.keywords), style="yellow")

    # Build content
    content = Group(
        get_status_text(),
        "",
        create_time_stats_table(),
        "",
        create_stream_stats_table(),
        "",
        create_filter_stats_table(),
        "",
        keywords_text,
        "",
        create_top_keywords_table(),
        "",
        create_top_languages_table(),
        "",
        create_top_hashtags_table(),
    )

    return Panel(
        content,
        title="[bold blue]❄️ Yeti - Bluesky Monitor[/bold blue]",
        subtitle="[dim]Paina Q lopettaaksesi[/dim]",
        border_style="blue",
        padding=(1, 2),
    )


def extract_hashtags(record: dict) -> list[str]:
    """Extract hashtags from post facets.

    Args:
        record: Post record dict

    Returns:
        List of hashtag strings (without # prefix)
    """
    hashtags = []
    for facet in record.get('facets', []):
        for feature in facet.get('features', []):
            if feature.get('$type') == 'app.bsky.richtext.facet#tag':
                tag = feature.get('tag', '')
                if tag and tag not in hashtags:
                    hashtags.append(tag)
    return hashtags


def extract_links(record: dict) -> list[str]:
    """Extract and validate external links from post facets.

    Args:
        record: Post record dict

    Returns:
        List of valid URLs
    """
    links = []
    for facet in record.get('facets', []):
        for feature in facet.get('features', []):
            if feature.get('$type') == 'app.bsky.richtext.facet#link':
                uri = feature.get('uri', '')
                if uri and uri not in links and validate_url(uri):
                    links.append(uri)
    return links


def fetch_bluesky_profile(did: str) -> dict:
    """Fetch display name from Bluesky API.

    Args:
        did: Decentralized identifier for the user

    Returns:
        Dict with 'displayName' and 'handle' keys
    """
    if not did:
        return {'displayName': None, 'handle': None}

    try:
        session = get_http_session()
        url = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"
        params = {'actor': did}

        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {
                'displayName': data.get('displayName'),
                'handle': data.get('handle')
            }
    except requests.exceptions.Timeout:
        logger.debug(f"Profile fetch timeout for {did}")
    except requests.exceptions.ConnectionError:
        logger.debug(f"Profile fetch connection error for {did}")
    except json.JSONDecodeError:
        logger.debug(f"Profile fetch JSON error for {did}")
    except Exception as e:
        logger.debug(f"Profile fetch error for {did}: {e}")

    return {'displayName': None, 'handle': None}


def extract_author_info(post_data):
    """Extract author information from post data."""
    did = post_data.get('did', '')
    # Handle could be extracted from DID or fetched via API
    return {
        'did': did,
        'handle': did.split(':')[-1] if did else 'unknown',
        'profile_url': f"https://bsky.app/profile/{did}" if did else None
    }


def display_analyzed_post(post_info):
    """Display a fully analyzed post with translation and news sources."""
    console.print(f"\n[cyan]{'═' * 70}[/cyan]")

    # Header with time and author
    header = Text()
    header.append(f"📅 {post_info['display_time']} ", style="yellow")
    header.append(f"({post_info['full_time']}) ", style="dim")
    if post_info.get('author_display_name'):
        header.append(f"👤 ", style="white")
        header.append(f"{post_info['author_display_name']}", style="bold cyan")
    console.print(header)

    # Language info
    lang_info = Text()
    lang_info.append("🌍 Kieli: ", style="white")
    lang_info.append(f"{post_info['language_name']}", style="magenta")
    lang_info.append(f" ({post_info['language_code']})", style="dim")
    console.print(lang_info)

    # Matched keywords
    if post_info.get('matched_keywords'):
        kw_text = Text()
        kw_text.append("🔑 Avainsanat: ", style="white")
        kw_text.append(", ".join(post_info['matched_keywords']), style="green")
        console.print(kw_text)

    # Hashtags
    if post_info.get('hashtags'):
        ht_text = Text()
        ht_text.append("# Hashtagit: ", style="white")
        ht_text.append(" ".join(f"#{t}" for t in post_info['hashtags'][:5]), style="blue")
        console.print(ht_text)

    console.print()

    # Original text
    console.print("[dim]─── Alkuperäinen teksti ───[/dim]")
    console.print(f"[white]{post_info['original_text']}[/white]")

    # Translation (if different from Finnish)
    if post_info.get('translation') and post_info['language_code'] != 'fi':
        console.print()
        console.print("[dim]─── Käännös suomeksi ───[/dim]")
        console.print(f"[bold bright_cyan]{post_info['translation']}[/bold bright_cyan]")

    # External links from post
    if post_info.get('links'):
        console.print()
        console.print("[dim]─── Linkit ───[/dim]")
        for link in post_info['links']:
            console.print(f"[bold bright_red]{link}[/bold bright_red]")

    console.print(f"[cyan]{'═' * 70}[/cyan]")


def process_post_analyzed(post_data, keywords, keywords_lower):
    """Process a post with full analysis: translation and news search.

    This function is meant to be run in a background thread.
    Returns a dict with all analyzed information.
    """
    global graceful_stop, quit_flag

    try:
        commit = post_data.get('commit', {})
        record = commit.get('record', {})
        text = record.get('text', '')
        langs = record.get('langs', [])
        created_at = record.get('createdAt', '')

        if quit_flag:
            return None

        # Parse timestamp
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00')).astimezone()
            full_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            display_time = dt.strftime('%H:%M:%S')
        except Exception:
            now = datetime.now()
            full_time = now.strftime('%Y-%m-%d %H:%M:%S')
            display_time = now.strftime('%H:%M:%S')

        # Get language info
        lang_code = langs[0] if langs else 'unknown'
        lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)

        # Get author info
        author = extract_author_info(post_data)

        # Get matched keywords
        text_lower = text.lower()
        if '*' in keywords:
            matched = ['*']
        else:
            matched = [kw for kw, kw_low in zip(keywords, keywords_lower) if kw_low in text_lower]

        # Get hashtags
        hashtags = extract_hashtags(record)

        # Get external links from post
        links = extract_links(record)

        # Fetch user's display name from Bluesky API
        profile = fetch_bluesky_profile(author['did'])
        display_name = profile.get('displayName') or profile.get('handle') or author['did']

        post_info = {
            'original_text': text,
            'display_time': display_time,
            'full_time': full_time,
            'language_code': lang_code,
            'language_name': lang_name,
            'author_did': author['did'],
            'author_handle': author['handle'],
            'author_display_name': display_name,
            'profile_url': author['profile_url'],
            'matched_keywords': matched,
            'hashtags': hashtags,
            'links': links,
            'translation': None
        }

        if quit_flag:
            return post_info

        # Step 1: Translate to Finnish (if not already Finnish)
        if lang_code != 'fi' and text:
            translation, detected_lang = translate_to_finnish(text)
            post_info['translation'] = translation
            if detected_lang and detected_lang != 'unknown':
                post_info['language_code'] = detected_lang
                post_info['language_name'] = LANGUAGE_NAMES.get(detected_lang, detected_lang)

        return post_info

    except Exception as e:
        return {
            'original_text': text if 'text' in dir() else '',
            'error': str(e),
            'display_time': datetime.now().strftime('%H:%M:%S'),
            'full_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'language_code': 'unknown',
            'language_name': 'Unknown',
            'matched_keywords': [],
            'hashtags': []
        }


async def process_queue_worker():
    """Background worker that processes posts from the queue."""
    global quit_flag, graceful_stop, post_queue, current_processing_post

    while True:
        if quit_flag:
            break

        if graceful_stop and post_queue.empty():
            break

        try:
            # Get item from queue with timeout
            try:
                item = post_queue.get(timeout=0.5)
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue

            post_data, keywords, keywords_lower = item
            current_processing_post = post_data

            # Process in thread pool to not block async loop
            loop = asyncio.get_event_loop()
            post_info = await loop.run_in_executor(
                None,
                process_post_analyzed,
                post_data,
                keywords,
                keywords_lower
            )

            current_processing_post = None

            if post_info and not quit_flag:
                display_analyzed_post(post_info)
                await asyncio.sleep(1)  # Tauko ennen seuraavaa

            post_queue.task_done()

        except Exception:
            current_processing_post = None
            await asyncio.sleep(0.1)

    # If graceful stop, process remaining items
    if graceful_stop and not quit_flag:
        while not post_queue.empty():
            try:
                item = post_queue.get_nowait()
                post_data, keywords, keywords_lower = item
                current_processing_post = post_data

                loop = asyncio.get_event_loop()
                post_info = await loop.run_in_executor(
                    None,
                    process_post_analyzed,
                    post_data,
                    keywords,
                    keywords_lower
                )

                current_processing_post = None

                if post_info:
                    display_analyzed_post(post_info)
                    await asyncio.sleep(1)  # Tauko ennen seuraavaa

                post_queue.task_done()
            except queue.Empty:
                break
            except Exception:
                current_processing_post = None


def archive_logs_and_reset():
    """Archive current logs and reset for new collection cycle."""
    global stats, log_files

    stats.finish()

    if log_files:
        stats.print_report()
        log_files.archive()
        log_files.close()

    console.print("\n[bold cyan]>>> Aloitetaan uusi keräyssykli...[/bold cyan]\n")

    # Reset
    keywords = stats.keywords
    stats = Statistics(keywords)
    log_files = LogFiles(source_type='live')


def check_run_limits():
    """Check if run limits have been reached."""
    global quit_flag, stats, run_time_limit, run_post_limit, auto_restart

    if quit_flag:
        return True

    if stats:
        if run_time_limit is not None:
            if stats.get_duration().total_seconds() >= run_time_limit:
                if auto_restart:
                    console.print("\n\n[bold yellow]>>> Aikaraja saavutettu. Arkistoidaan lokit...[/bold yellow]")
                    archive_logs_and_reset()
                    return False
                else:
                    console.print("\n\n[bold yellow]>>> Aikaraja saavutettu. Sammutetaan...[/bold yellow]\n")
                    quit_flag = True
                    return True

        if run_post_limit is not None:
            if stats.displayed_posts >= run_post_limit:
                console.print(f"\n\n[bold yellow]>>> Postiraja saavutettu ({run_post_limit:,} postiä). Sammutetaan...[/bold yellow]\n")
                quit_flag = True
                return True

    return False


def display_post(post_data: dict, keywords: list[str], keywords_lower: list[str],
                 silent_mode: bool = False, analyzed_mode: bool = False) -> None:
    """Display and process a post.

    Args:
        post_data: The post data from Jetstream
        keywords: List of keywords to match
        keywords_lower: Pre-computed lowercase keywords
        silent_mode: If True, only track stats, don't display
        analyzed_mode: If True, queue post for translation/analysis
    """
    global stats, log_files, post_queue

    try:
        commit = post_data.get('commit', {})
        record = commit.get('record', {})
        text = record.get('text', '')
        langs = record.get('langs', [])
        created_at = record.get('createdAt', '')

        # Parse timestamp
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00')).astimezone()
            time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            display_time = dt.strftime('%H:%M:%S')
        except Exception:
            now = datetime.now()
            time_str = now.strftime('%Y-%m-%d %H:%M:%S')
            display_time = now.strftime('%H:%M:%S')

        if stats:
            stats.record_post()

        # Extract hashtags for statistics
        hashtags = extract_hashtags(record)
        if hashtags and stats:
            stats.record_all_hashtags(hashtags)

        # Log all posts to ALL file
        if log_files and text:
            log_files.log_all(text, time_str)

        if not text:
            return

        # Check keyword matches (pre-computed lowercase)
        # Special case: '*' matches all posts
        text_lower = text.lower()
        if '*' in keywords:
            matched = ['*']
        else:
            matched = [kw for kw, kw_low in zip(keywords, keywords_lower) if kw_low in text_lower]
            if not matched:
                return

        # Track filtered statistics
        if stats:
            stats.record_language(langs)
            stats.record_displayed(text_lower, keywords)
            if hashtags:
                stats.record_hashtags(hashtags)

        if silent_mode:
            return

        # Analyzed mode: queue post for processing
        if analyzed_mode:
            post_queue.put((post_data, keywords, keywords_lower))
            return

        # Display post (normal mode)
        lang_code = langs[0] if langs else 'unknown'
        lang_name = LANGUAGE_NAMES.get(lang_code, lang_code) if lang_code != 'unknown' else 'Unknown'
        console.print(f"\n[cyan]{'─' * 60}[/cyan]")
        console.print(f"[yellow][{display_time}][/yellow] [green]{', '.join(matched)}[/green] [magenta][{lang_name}][/magenta]")
        console.print(f"\n[white]{text}[/white]")

    except Exception:
        pass


# Global reference for Rich Live display
live_display = None


def update_rich_display():
    """Update the Rich Live display."""
    global live_display
    if live_display:
        live_display.update(create_display())


async def check_keyboard():
    """Check for keyboard input (Q to quit immediately, S for graceful stop)."""
    global quit_flag, graceful_stop

    while not quit_flag and not graceful_stop:
        key = check_key_pressed()
        if key:
            if key == 'q':
                console.print("\n[bold red]>>> Q painettu - Lopetetaan heti...[/bold red]")
                quit_flag = True
                break
            elif key == 's':
                console.print("\n[bold yellow]>>> S painettu - Lopetetaan kun nykyinen työ valmis...[/bold yellow]")
                graceful_stop = True
                break
        await asyncio.sleep(0.05)


async def websocket_ping(websocket, interval=30):
    """Send periodic ping to keep WebSocket connection alive."""
    global quit_flag

    while not quit_flag:
        try:
            await asyncio.sleep(interval)
            if not quit_flag:
                await websocket.ping()
        except Exception:
            break


async def update_live_stats(interval=0.5):
    """Update live statistics display periodically using Rich."""
    global quit_flag

    while not quit_flag:
        update_rich_display()
        await asyncio.sleep(interval)


async def monitor_jetstream(keywords: list[str], keywords_lower: list[str],
                           silent_mode: bool = False, analyzed_mode: bool = False) -> None:
    """Connect to Jetstream and monitor for posts."""
    global quit_flag, graceful_stop

    mode_text = "analysoitu" if analyzed_mode else ("tausta" if silent_mode else "normaali")

    if not silent_mode or analyzed_mode:
        console.print("\n[bold white]Yhdistetään Bluesky Jetstreamiin...[/bold white]")
        console.print(f"[yellow]Monitoroidaan avainsanoja: {', '.join(keywords)}[/yellow]")
        console.print(f"[cyan]Moodi: {mode_text}[/cyan]")
        console.print("[cyan]Paina Q lopettaaksesi heti, S lopettaaksesi kun työ valmis[/cyan]\n")

    while not quit_flag and not graceful_stop:
        try:
            async with websockets.connect(JETSTREAM_URL, ping_interval=30, ping_timeout=10) as ws:
                if not silent_mode or analyzed_mode:
                    console.print("[bold green]✓ Yhdistetty! Odotetaan posteja...[/bold green]")

                ping_task = asyncio.create_task(websocket_ping(ws, 30))

                try:
                    while not quit_flag and not graceful_stop and not check_run_limits():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                            data = safe_json_loads(msg)
                            if data is None:
                                continue

                            if data.get('kind') == 'commit':
                                commit = data.get('commit', {})
                                if commit.get('operation') == 'create' and commit.get('collection') == 'app.bsky.feed.post':
                                    display_post(data, keywords, keywords_lower, silent_mode, analyzed_mode)
                                    check_run_limits()

                        except asyncio.TimeoutError:
                            check_run_limits()
                        except websockets.exceptions.ConnectionClosedError:
                            logger.debug("WebSocket connection closed")
                            break
                        except Exception as e:
                            logger.debug(f"WebSocket receive error: {e}")
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosed:
            if not quit_flag and not graceful_stop and (not silent_mode or analyzed_mode):
                console.print("\n[yellow]Yhteys katkesi. Yhdistetään uudelleen 5 sekunnissa...[/yellow]")
            await asyncio.sleep(5)
        except Exception as e:
            if not quit_flag and not graceful_stop and (not silent_mode or analyzed_mode):
                console.print(f"\n[yellow]Yhteysvirhe: {e}. Yhdistetään uudelleen 5 sekunnissa...[/yellow]")
            await asyncio.sleep(5)


async def periodic_flush(interval=5):
    """Periodically flush all log files."""
    global quit_flag, log_files

    while not quit_flag:
        await asyncio.sleep(interval)
        if log_files:
            log_files.flush_all()


async def run_monitor_inner(keywords, keywords_lower, silent_mode=False, analyzed_mode=False):
    """Inner monitor loop without Rich Live management."""
    global quit_flag, graceful_stop

    tasks = [
        asyncio.create_task(monitor_jetstream(keywords, keywords_lower, silent_mode, analyzed_mode)),
        asyncio.create_task(check_keyboard()),
        asyncio.create_task(periodic_flush(5)),
    ]

    if silent_mode:
        tasks.append(asyncio.create_task(update_live_stats(0.5)))

    # Add queue worker for analyzed mode
    if analyzed_mode:
        tasks.append(asyncio.create_task(process_queue_worker()))

    while not quit_flag and not graceful_stop:
        await asyncio.wait([t for t in tasks if not t.done()], timeout=0.5, return_when=asyncio.FIRST_COMPLETED)

    # If graceful stop, wait for queue to finish
    if graceful_stop and not quit_flag and analyzed_mode:
        console.print("\n[yellow]Odotetaan jonon käsittelyn valmistumista...[/yellow]")
        # Keep queue worker running
        queue_task = [t for t in tasks if not t.done()]
        if queue_task:
            await asyncio.wait(queue_task, timeout=300)  # Max 5 min wait

    for task in tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


async def run_monitor(keywords, keywords_lower, silent_mode=False, analyzed_mode=False):
    """Run the monitor with Rich Live display in background mode."""
    global quit_flag, live_display

    if silent_mode:
        # Use Rich Live display for background mode
        with Live(create_display(), refresh_per_second=4, console=console) as live:
            live_display = live
            await run_monitor_inner(keywords, keywords_lower, silent_mode, analyzed_mode)
            live_display = None
    else:
        # Regular mode (or analyzed mode) without Rich Live
        await run_monitor_inner(keywords, keywords_lower, silent_mode, analyzed_mode)


def find_date_log_files():
    """Find all date-based log files (YYYY-MM-DD.log) in search directories.

    Searches in:
    1. Current directory
    2. LOGS subdirectory
    3. YETI_LOG_DIR environment variable (if set)

    Returns: List of (path, date_string) tuples, sorted newest first
    """
    found_files = []

    # Search directories (configurable via environment variable)
    extra_search_dir = os.environ.get('YETI_LOG_DIR', '')
    search_dirs = [
        '.',  # Current directory
        'LOGS',  # LOGS subdirectory
    ]
    if extra_search_dir and os.path.isdir(extra_search_dir):
        search_dirs.append(extra_search_dir)

    for search_dir in search_dirs:
        if not os.path.isdir(search_dir):
            continue

        for filename in os.listdir(search_dir):
            match = DATE_LOG_PATTERN.match(filename)
            if match:
                date_str = match.group(1)
                full_path = os.path.join(search_dir, filename)
                if os.path.isfile(full_path):
                    # Normalize path
                    if search_dir == '.':
                        full_path = filename
                    found_files.append((full_path, date_str))

    # Sort by date (newest first)
    found_files.sort(key=lambda x: x[1], reverse=True)

    # Remove duplicates (same date from different dirs - prefer local)
    seen_dates = set()
    unique_files = []
    for path, date_str in found_files:
        if date_str not in seen_dates:
            seen_dates.add(date_str)
            unique_files.append((path, date_str))

    return unique_files


def check_date_log_file():
    """Check if a date-based log file exists (YYYY-MM-DD.log).

    Returns: path to the newest file if exists, None otherwise
    """
    files = find_date_log_files()
    if files:
        return files[0][0]  # Return path of newest file
    return None


async def process_json_log_file(log_path, keywords, keywords_lower, silent_mode=False, analyzed_mode=False, start_position=0):
    """Process posts from a JSON log file instead of live Jetstream.

    The log file is expected to have one JSON object per line,
    in the same format as Jetstream messages.

    Exits when end of file is reached.

    Args:
        start_position: File position to start reading from (used after prescan)
    """
    global quit_flag, graceful_stop, stats, log_files

    console.print(f"\n[bold cyan]📂 Luetaan dataa tiedostosta: {log_path}[/bold cyan]")
    console.print(f"[yellow]Monitoroidaan avainsanoja: {', '.join(keywords)}[/yellow]")
    console.print("[cyan]Paina Q lopettaaksesi[/cyan]\n")

    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            # Seek to start position if provided (after prescan)
            if start_position > 0:
                f.seek(start_position)

            line_count = 0

            while not quit_flag and not graceful_stop:
                line = f.readline()

                if not line:
                    # End of file reached - exit
                    console.print(f"\n[bold green]✓ Tiedosto käsitelty: {line_count} riviä[/bold green]")
                    quit_flag = True
                    break

                line = line.strip()
                if not line:
                    continue

                # Skip overly long lines
                if len(line) > MAX_LINE_LENGTH:
                    continue

                try:
                    data = safe_json_loads(line)
                    if data is None:
                        continue

                    # Handle both direct post format and Jetstream format
                    if data.get('kind') == 'commit':
                        # Jetstream format
                        commit = data.get('commit', {})
                        if commit.get('operation') == 'create' and commit.get('collection') == 'app.bsky.feed.post':
                            display_post(data, keywords, keywords_lower, silent_mode, analyzed_mode)
                    elif 'commit' in data:
                        # Already has commit structure
                        display_post(data, keywords, keywords_lower, silent_mode, analyzed_mode)
                    elif 'record' in data or 'text' in data:
                        # Direct record format - wrap it
                        wrapped = {
                            'kind': 'commit',
                            'did': data.get('did', ''),
                            'commit': {
                                'operation': 'create',
                                'collection': 'app.bsky.feed.post',
                                'record': data if 'text' in data else data.get('record', data)
                            }
                        }
                        display_post(wrapped, keywords, keywords_lower, silent_mode, analyzed_mode)

                    line_count += 1

                    # Small delay to allow keyboard check and display update
                    if line_count % 100 == 0:
                        await asyncio.sleep(0.01)

                except json.JSONDecodeError:
                    continue
                except Exception:
                    continue

    except FileNotFoundError:
        console.print(f"[bold red]✗ Tiedostoa ei löydy: {log_path}[/bold red]")
    except Exception as e:
        console.print(f"[bold red]✗ Virhe tiedoston lukemisessa: {e}[/bold red]")


async def run_file_monitor(log_path, keywords, keywords_lower, silent_mode=False, analyzed_mode=False, start_position=0):
    """Run the monitor reading from a file instead of Jetstream."""
    global quit_flag, live_display

    tasks = [
        asyncio.create_task(process_json_log_file(log_path, keywords, keywords_lower, silent_mode, analyzed_mode, start_position)),
        asyncio.create_task(check_keyboard()),
    ]

    if silent_mode:
        tasks.append(asyncio.create_task(update_live_stats(0.5)))

    if analyzed_mode:
        tasks.append(asyncio.create_task(process_queue_worker()))

    await asyncio.gather(*tasks, return_exceptions=True)

    for task in tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


async def run_file_monitor_with_display(log_path, keywords, keywords_lower, silent_mode=False, analyzed_mode=False, start_position=0):
    """Run file monitor with Rich Live display in background mode."""
    global live_display

    if silent_mode:
        with Live(create_display(), refresh_per_second=4, console=console) as live:
            live_display = live
            await run_file_monitor(log_path, keywords, keywords_lower, silent_mode, analyzed_mode, start_position)
            live_display = None
    else:
        await run_file_monitor(log_path, keywords, keywords_lower, silent_mode, analyzed_mode, start_position)


def load_keywords_from_file():
    """Load keywords from keywords.txt if it exists."""
    if os.path.exists('keywords.txt'):
        with open('keywords.txt', 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    return []


def save_keywords_to_file(keywords):
    """Save keywords to keywords.txt."""
    with open('keywords.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(keywords) + '\n')


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Yeti - Bluesky Jetstream Real-time Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esimerkkejä:
  python yeti.py                      # Interaktiivinen käyttö
  python yeti.py -f data.log          # Lue dataa tiedostosta
  python yeti.py --file /path/to.log  # Lue dataa absoluuttisesta polusta
        """
    )
    parser.add_argument(
        '-f', '--file',
        type=str,
        metavar='TIEDOSTO',
        help='Lue dataa JSON-lokitiedostosta Jetstreamin sijaan'
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    global stats, quit_flag, graceful_stop, log_files, run_time_limit, run_post_limit, auto_restart, data_source

    # Parse command-line arguments
    args = parse_arguments()

    # Welcome banner
    console.print()
    console.print(Panel(
        "[bold white]Bluesky Jetstream Real-time Monitor[/bold white]\n"
        "[dim]Suodata posteja avainsanojen perusteella[/dim]",
        title="[bold blue]❄️ Yeti[/bold blue]",
        border_style="blue",
        padding=(1, 2)
    ))
    console.print()

    # Cleanup old log files at startup
    cleanup_logs_directory()

    # Load or input keywords
    keywords = []
    saved = load_keywords_from_file()

    if saved:
        # Show saved keywords in a panel
        kw_text = Text()
        for i, kw in enumerate(saved, 1):
            kw_text.append(f"  {i}. ", style="dim")
            kw_text.append(f"{kw}\n", style="bold cyan")
        console.print(Panel(
            kw_text,
            title="[bold cyan]📋 Tallennetut avainsanat[/bold cyan]",
            border_style="cyan",
            width=60,
            padding=(0, 2)
        ))

        use_saved = console.input("[cyan]Käytä tallennettuja avainsanoja? [Y/n]: [/cyan]").strip().lower()
        if use_saved != 'n':
            keywords = saved
            console.print("[green]✓ Käytetään tallennettuja avainsanoja.[/green]\n")
        else:
            console.print("\n[cyan]Syötä uudet avainsanat (tyhjä rivi lopettaa):[/cyan]")

    if not keywords:
        if not saved:
            console.print("[cyan]Syötä avainsanat monitorointiin (tyhjä rivi lopettaa):[/cyan]")

        while True:
            kw = console.input(f"  [dim]Avainsana {len(keywords) + 1}:[/dim] ").strip()
            if not kw:
                break
            keywords.append(kw)

        if not keywords:
            console.print("[yellow]Ei avainsanoja. Lopetetaan.[/yellow]")
            return

        save_keywords_to_file(keywords)
        console.print("[green]✓ Avainsanat tallennettu: keywords.txt[/green]")

    keywords_lower = [kw.lower() for kw in keywords]
    console.print(f"\n[bold white]🔍 Avainsanat:[/bold white] [yellow]{', '.join(keywords)}[/yellow]\n")

    # Determine data source
    use_file_source = False
    file_to_use = None

    # Command-line argument takes priority
    if args.file:
        validated_path = safe_path(args.file)
        if validated_path:
            use_file_source = True
            file_to_use = validated_path
            data_source = validated_path
            console.print(f"[green]✓ Käytetään tiedostoa: {validated_path}[/green]\n")
        else:
            console.print(f"[bold red]✗ Tiedostoa ei löydy tai polku ei kelpaa: {args.file}[/bold red]")
            return
    else:
        # Find newest date-based log file
        date_log_files = find_date_log_files()
        newest_log = date_log_files[0] if date_log_files else None

        if newest_log:
            # Log file found - offer choice, default to file
            path, date_str = newest_log
            file_size = os.path.getsize(path)
            size_str = f"{file_size / 1024 / 1024:.1f} MB" if file_size > 1024*1024 else f"{file_size / 1024:.1f} KB"

            console.print("[bold cyan]📡 Datalähde:[/bold cyan]")
            console.print(f"  [white]1[/white] - 📂 {path} [dim]({date_str}, {size_str})[/dim] [bold](oletus)[/bold]")
            console.print("  [white]2[/white] - Live Jetstream (reaaliaikainen)")

            source_choice = console.input("\n[cyan]Valitse datalähde [1]: [/cyan]").strip()

            if source_choice == "2":
                data_source = 'jetstream'
                console.print("[green]✓ Käytetään live Jetstream -yhteyttä[/green]\n")
            else:
                use_file_source = True
                file_to_use = path
                data_source = file_to_use
                console.print(f"[green]✓ Käytetään tiedostoa: {file_to_use}[/green]\n")
        else:
            # No log file found - use live
            data_source = 'jetstream'
            console.print("[dim]Lokitiedostoja ei löytynyt. Käytetään live Jetstream -yhteyttä.[/dim]\n")

    run_time_limit = None
    run_post_limit = None

    # Run mode (only for live stream)
    if not use_file_source:
        console.print("[bold cyan]⏱️  Ajomoodi:[/bold cyan]")
        console.print("  [white]1[/white] - Jatkuva (kunnes keskeytetään) [dim](oletus)[/dim]")
        console.print("  [white]2[/white] - Aikarajoitettu")
        console.print("  [white]3[/white] - Postirajoitettu")
        run_mode = console.input("\n[cyan]Valitse moodi [1]: [/cyan]").strip()

        if run_mode == "2":
            console.print("\n[cyan]Syötä kesto:[/cyan]")
            try:
                h = int(console.input("  [dim]Tunnit [0]:[/dim] ").strip() or "0")
                m = int(console.input("  [dim]Minuutit [0]:[/dim] ").strip() or "0")
                s = int(console.input("  [dim]Sekunnit [0]:[/dim] ").strip() or "0")
                run_time_limit = h * 3600 + m * 60 + s
                if run_time_limit <= 0:
                    console.print("[yellow]⚠️  Virheellinen kesto. Jatketaan kunnes keskeytetään.[/yellow]")
                    run_time_limit = None
                else:
                    # Ask about auto-restart
                    console.print("\n[bold cyan]🔄 Aikarajan jälkeen:[/bold cyan]")
                    console.print("  [white]1[/white] - Uudelleenkäynnistys (arkistoi ja jatkaa) [dim](oletus)[/dim]")
                    console.print("  [white]2[/white] - Aja vain kerran (pysähdy aikarajan jälkeen)")
                    restart_choice = console.input("\n[cyan]Valitse [1]: [/cyan]").strip()
                    auto_restart = restart_choice != "2"

                    if auto_restart:
                        console.print(f"[green]✓ Keräyssykli: {h:02d}:{m:02d}:{s:02d} (arkistoi ja jatkaa)[/green]")
                    else:
                        console.print(f"[green]✓ Ajetaan {h:02d}:{m:02d}:{s:02d} ja pysähdytään[/green]")
            except ValueError:
                console.print("[yellow]⚠️  Virheellinen syöte. Jatketaan kunnes keskeytetään.[/yellow]")

        elif run_mode == "3":
            try:
                count = int(console.input("\n[cyan]Kerättävien postien määrä: [/cyan]").strip())
                if count > 0:
                    run_post_limit = count
                    console.print(f"[green]✓ Kerätään {count:,} postiä[/green]")
                else:
                    console.print("[yellow]⚠️  Virheellinen määrä. Jatketaan kunnes keskeytetään.[/yellow]")
            except ValueError:
                console.print("[yellow]⚠️  Virheellinen syöte. Jatketaan kunnes keskeytetään.[/yellow]")

    # Display mode (only for live stream)
    silent_mode = True
    analyzed_mode = False

    if not use_file_source:
        console.print("\n[bold cyan]🖥️  Näyttömoodi:[/bold cyan]")
        console.print("  [white]1[/white] - Näytä suodatetut postit (alkuperäinen teksti)")
        console.print("  [white]2[/white] - Taustamoodi (vain live-tilastot) [dim](oletus)[/dim]")
        console.print("  [white]3[/white] - Käännetty moodi (suomennos)")

        display_choice = console.input("\n[cyan]Valitse moodi [2]: [/cyan]").strip()
        silent_mode = display_choice in ("2", "")
        analyzed_mode = display_choice == "3"

        if analyzed_mode:
            console.print("\n[bold green]✓ Analysoitu moodi valittu[/bold green]")
            console.print("[dim]  • Postaukset käännetään suomeksi[/dim]")
            console.print("[dim]  • Q = lopeta heti, S = lopeta kun työ valmis[/dim]")

    # Initialize
    stats = Statistics(keywords)
    source_type = 'file' if use_file_source else 'live'
    log_files = LogFiles(source_type=source_type)
    quit_flag = False
    graceful_stop = False
    file_start_position = 0

    if use_file_source:
        # Process file: analyze and show stats
        prescan_data_file(file_to_use, keywords, keywords_lower, stats, log_files)
        stats.finish()
        stats.print_report()
        if log_files:
            log_files.archive()
            log_files.close()
    else:
        # Live stream: show logging info and run monitor
        log_table = Table(title="📁 Lokitiedostot", show_header=False, box=None)
        log_table.add_column("", style="dim")
        log_table.add_row(f"  {log_files.all_filename} - Suodatetut postit")
        console.print()
        console.print(log_table)
        console.print()

        old_settings = setup_terminal()

        try:
            asyncio.run(run_monitor(keywords, keywords_lower, silent_mode, analyzed_mode))
        except KeyboardInterrupt:
            pass
        finally:
            restore_terminal(old_settings)
            stats.finish()
            console.print()  # Clear line after Live display
            stats.print_report()
            if log_files:
                log_files.archive()
                log_files.close()


if __name__ == "__main__":
    main()
