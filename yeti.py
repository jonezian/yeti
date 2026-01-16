#!/usr/bin/env python3
"""
Bluesky Jetstream Monitor - Real-time post monitoring with keyword filtering
Connects to Bluesky Jetstream WebSocket service and displays posts matching keywords.
Non-English posts are automatically translated to English.
"""

import json
import asyncio
import websockets
import requests
import sys
import select
import tty
import termios
import os
import shutil
from datetime import datetime
from collections import defaultdict

# ANSI color codes (bright versions)
BRIGHT_LIGHT_BLUE = "\033[1;96m"  # Bright cyan for translations
RESET = "\033[0m"
BRIGHT_CYAN = "\033[1;36m"
BRIGHT_YELLOW = "\033[1;93m"
BRIGHT_WHITE = "\033[1;97m"
BRIGHT_RED = "\033[1;91m"  # Bright red for links
BRIGHT_GREEN = "\033[1;92m"
BRIGHT_MAGENTA = "\033[1;95m"

# Jetstream WebSocket endpoint
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

# Language code to full name mapping
LANGUAGE_NAMES = {
    'af': 'Afrikaans',
    'am': 'Amharic',
    'ar': 'Arabic',
    'az': 'Azerbaijani',
    'be': 'Belarusian',
    'bg': 'Bulgarian',
    'bn': 'Bengali',
    'bs': 'Bosnian',
    'ca': 'Catalan',
    'cs': 'Czech',
    'cy': 'Welsh',
    'da': 'Danish',
    'de': 'German',
    'el': 'Greek',
    'en': 'English',
    'eo': 'Esperanto',
    'es': 'Spanish',
    'et': 'Estonian',
    'eu': 'Basque',
    'fa': 'Persian',
    'fi': 'Finnish',
    'fr': 'French',
    'ga': 'Irish',
    'gl': 'Galician',
    'gu': 'Gujarati',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'hr': 'Croatian',
    'hu': 'Hungarian',
    'hy': 'Armenian',
    'id': 'Indonesian',
    'is': 'Icelandic',
    'it': 'Italian',
    'ja': 'Japanese',
    'ka': 'Georgian',
    'kk': 'Kazakh',
    'km': 'Khmer',
    'kn': 'Kannada',
    'ko': 'Korean',
    'ku': 'Kurdish',
    'ky': 'Kyrgyz',
    'la': 'Latin',
    'lo': 'Lao',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'mk': 'Macedonian',
    'ml': 'Malayalam',
    'mn': 'Mongolian',
    'mr': 'Marathi',
    'ms': 'Malay',
    'mt': 'Maltese',
    'my': 'Burmese',
    'ne': 'Nepali',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'pa': 'Punjabi',
    'pl': 'Polish',
    'ps': 'Pashto',
    'pt': 'Portuguese',
    'ro': 'Romanian',
    'ru': 'Russian',
    'sd': 'Sindhi',
    'si': 'Sinhala',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'so': 'Somali',
    'sq': 'Albanian',
    'sr': 'Serbian',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'tg': 'Tajik',
    'th': 'Thai',
    'tl': 'Tagalog',
    'tr': 'Turkish',
    'uk': 'Ukrainian',
    'ur': 'Urdu',
    'uz': 'Uzbek',
    'vi': 'Vietnamese',
    'yi': 'Yiddish',
    'zh': 'Chinese',
    'zu': 'Zulu',
    'unknown': 'Unknown',
}


class Statistics:
    """Track statistics for the monitoring session."""

    def __init__(self, keywords):
        self.keywords = keywords
        self.start_time = datetime.now()
        self.end_time = None
        self.total_posts = 0
        self.displayed_posts = 0
        self.keyword_counts = defaultdict(int)
        self.language_counts = defaultdict(int)
        # Posts per second tracking
        self.posts_per_second = 0
        self.current_second_posts = 0
        self.last_second_time = datetime.now()

    def record_post(self, langs):
        """Record a post from the stream."""
        self.total_posts += 1

        # Update posts per second
        now = datetime.now()
        if (now - self.last_second_time).total_seconds() >= 1.0:
            self.posts_per_second = self.current_second_posts
            self.current_second_posts = 1
            self.last_second_time = now
        else:
            self.current_second_posts += 1

        # Track languages
        if langs:
            for lang in langs:
                self.language_counts[lang] += 1
        else:
            self.language_counts['unknown'] += 1

    def record_displayed(self, text, keywords):
        """Record a displayed post and which keywords matched."""
        self.displayed_posts += 1
        text_lower = text.lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                self.keyword_counts[kw] += 1

    def finish(self):
        """Mark the session as finished."""
        self.end_time = datetime.now()

    def get_duration(self):
        """Get the duration of the session."""
        end = self.end_time or datetime.now()
        return end - self.start_time

    def print_report(self):
        """Print the statistics report and save to file."""
        duration = self.get_duration()
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        # Build report lines for both screen and file
        report_lines = []

        report_lines.append("=" * 60)
        report_lines.append("                    SESSION REPORT")
        report_lines.append("=" * 60)
        report_lines.append("")

        # Time info
        report_lines.append("Time:")
        report_lines.append(f"  Started:  {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"  Ended:    {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"  Duration: {hours:02d}:{minutes:02d}:{seconds:02d}")

        # Post counts
        report_lines.append("")
        report_lines.append("Posts:")
        report_lines.append(f"  Total from stream: {self.total_posts:,}")
        report_lines.append(f"  Displayed:         {self.displayed_posts:,}")
        if self.total_posts > 0:
            percentage = (self.displayed_posts / self.total_posts) * 100
            report_lines.append(f"  Match rate:        {percentage:.2f}%")

        # Keyword stats
        report_lines.append("")
        report_lines.append("Keyword matches:")
        if self.keyword_counts:
            # Sort keywords by match count (highest first)
            sorted_keywords = sorted(self.keywords, key=lambda kw: self.keyword_counts.get(kw, 0), reverse=True)
            for kw in sorted_keywords:
                count = self.keyword_counts.get(kw, 0)
                report_lines.append(f"  {kw}: {count:,}")
        else:
            report_lines.append("  No matches")

        # Language stats
        report_lines.append("")
        report_lines.append("Languages:")
        if self.language_counts:
            sorted_langs = sorted(self.language_counts.items(), key=lambda x: x[1], reverse=True)
            for lang_code, count in sorted_langs[:15]:
                percentage = (count / self.total_posts) * 100 if self.total_posts > 0 else 0
                lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
                report_lines.append(f"  {lang_name}: {count:,} ({percentage:.1f}%)")
            if len(sorted_langs) > 15:
                report_lines.append(f"  ... and {len(sorted_langs) - 15} more languages")
        else:
            report_lines.append("  No language data")

        report_lines.append("")
        report_lines.append("=" * 60)

        # Save to file
        with open('report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        # Print to screen with colors
        print(f"\n\n{BRIGHT_CYAN}{'═' * 60}{RESET}")
        print(f"{BRIGHT_CYAN}                    SESSION REPORT{RESET}")
        print(f"{BRIGHT_CYAN}{'═' * 60}{RESET}\n")

        print(f"{BRIGHT_WHITE}Time:{RESET}")
        print(f"  Started:  {BRIGHT_YELLOW}{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Ended:    {BRIGHT_YELLOW}{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Duration: {BRIGHT_YELLOW}{hours:02d}:{minutes:02d}:{seconds:02d}{RESET}")

        print(f"\n{BRIGHT_WHITE}Posts:{RESET}")
        print(f"  Total from stream: {BRIGHT_GREEN}{self.total_posts:,}{RESET}")
        print(f"  Displayed:         {BRIGHT_GREEN}{self.displayed_posts:,}{RESET}")
        if self.total_posts > 0:
            percentage = (self.displayed_posts / self.total_posts) * 100
            print(f"  Match rate:        {BRIGHT_GREEN}{percentage:.2f}%{RESET}")

        print(f"\n{BRIGHT_WHITE}Keyword matches:{RESET}")
        if self.keyword_counts:
            # Sort keywords by match count (highest first)
            sorted_keywords = sorted(self.keywords, key=lambda kw: self.keyword_counts.get(kw, 0), reverse=True)
            for kw in sorted_keywords:
                count = self.keyword_counts.get(kw, 0)
                print(f"  {BRIGHT_YELLOW}{kw}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No matches{RESET}")

        print(f"\n{BRIGHT_WHITE}Languages:{RESET}")
        if self.language_counts:
            sorted_langs = sorted(self.language_counts.items(), key=lambda x: x[1], reverse=True)
            for lang_code, count in sorted_langs[:15]:
                percentage = (count / self.total_posts) * 100 if self.total_posts > 0 else 0
                lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
                print(f"  {BRIGHT_MAGENTA}{lang_name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} ({percentage:.1f}%)")
            if len(sorted_langs) > 15:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_langs) - 15} more languages{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No language data{RESET}")

        print(f"\n{BRIGHT_CYAN}{'═' * 60}{RESET}")
        print(f"{BRIGHT_GREEN}Report saved to report.txt{RESET}\n")


# Global statistics instance
stats = None
# Global flag for quit
quit_flag = False
# Global flag for graceful shutdown (stop posts, continue URL analysis)
graceful_shutdown = False
# Global log files
log_files = None
# Global run limits
run_time_limit = None  # In seconds, None = unlimited
run_post_limit = None  # Number of filtered posts, None = unlimited


def archive_logs_and_reset():
    """Archive current logs and reset for new collection cycle."""
    global stats, log_files, url_stats

    # Finish current statistics
    stats.finish()
    stats.print_report()

    # Close current log files
    if log_files:
        log_files.close()

    # Create archive directory with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_dir = f"{timestamp}_logs_archive"
    os.makedirs(archive_dir, exist_ok=True)

    # Move log files and report to archive
    files_to_archive = ['full.log', 'posts.log', 'translated.log', 'URLs.log', 'url-analysis.log', 'report.txt']
    for filename in files_to_archive:
        if os.path.exists(filename):
            shutil.move(filename, os.path.join(archive_dir, filename))

    print(f"\n{BRIGHT_GREEN}>>> Logs archived to: {archive_dir}/{RESET}")
    print(f"{BRIGHT_CYAN}>>> Starting new collection cycle...{RESET}\n")

    # Reset statistics with same keywords
    keywords = stats.keywords
    stats = Statistics(keywords)

    # Reset URL stats
    url_stats['analyzed'] = 0
    url_stats['skipped'] = 0

    # Create new log files (skip backup since we just archived)
    log_files = LogFiles(skip_backup=True)


def check_run_limits():
    """Check if run limits have been reached and set quit_flag if so."""
    global quit_flag, stats, run_time_limit, run_post_limit

    if quit_flag:
        return True

    if stats:
        # Check time limit - archive and restart instead of quitting
        if run_time_limit is not None:
            duration = stats.get_duration()
            if duration.total_seconds() >= run_time_limit:
                print(f"\n\n{BRIGHT_YELLOW}>>> Time limit reached. Archiving logs...{RESET}")
                archive_logs_and_reset()
                return False  # Continue monitoring with fresh state

        # Check post limit - this still quits
        if run_post_limit is not None:
            if stats.displayed_posts >= run_post_limit:
                print(f"\n\n{BRIGHT_YELLOW}>>> Post limit reached ({run_post_limit:,} posts). Shutting down...{RESET}\n")
                quit_flag = True
                return True

    return False


class LogFiles:
    """Handle logging to multiple files."""

    LOG_FILES = ['full.log', 'posts.log', 'translated.log', 'URLs.log', 'url-analysis.log', 'report.txt']

    def __init__(self, skip_backup=False):
        # Check if any log files exist and backup them (unless skipped)
        if not skip_backup:
            self._backup_existing_logs()

        # Create new log files
        self.full_log = open('full.log', 'w', encoding='utf-8')
        self.posts_log = open('posts.log', 'w', encoding='utf-8')
        self.translated_log = open('translated.log', 'w', encoding='utf-8')
        self.urls_log = open('URLs.log', 'w', encoding='utf-8')
        self.url_analysis_log = open('url-analysis.log', 'w', encoding='utf-8')

    def _backup_existing_logs(self):
        """Backup existing log files to a timestamped directory."""
        # Check if any log files exist
        existing_files = [f for f in self.LOG_FILES if os.path.exists(f)]

        if not existing_files:
            return

        # Create backup directory with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = f"{timestamp}_logs"
        os.makedirs(backup_dir, exist_ok=True)

        # Move existing files to backup directory
        for log_file in existing_files:
            shutil.move(log_file, os.path.join(backup_dir, log_file))

        print(f"{BRIGHT_YELLOW}Existing logs moved to: {backup_dir}/{RESET}")

    def log_full(self, text, timestamp):
        """Log all posts from stream."""
        self.full_log.write(f"[{timestamp}] {text}\n")
        self.full_log.flush()

    def log_post(self, text, timestamp):
        """Log filtered posts."""
        self.posts_log.write(f"[{timestamp}] {text}\n")
        self.posts_log.flush()

    def log_translated(self, original, translation, timestamp):
        """Log translated posts."""
        self.translated_log.write(f"[{timestamp}]\n")
        self.translated_log.write(f"Original: {original}\n")
        self.translated_log.write(f"English: {translation}\n\n")
        self.translated_log.flush()

    def log_url(self, url):
        """Log external URLs only."""
        self.urls_log.write(f"{url}\n")
        self.urls_log.flush()

    def log_url_analysis(self, url, analysis, timestamp):
        """Log URL analysis from gemini-cli."""
        self.url_analysis_log.write(f"{'=' * 60}\n")
        self.url_analysis_log.write(f"[{timestamp}]\n")
        self.url_analysis_log.write(f"URL: {url}\n")
        self.url_analysis_log.write(f"Analysis:\n{analysis}\n\n")
        self.url_analysis_log.flush()

    def close(self):
        """Close all log files."""
        self.full_log.close()
        self.posts_log.close()
        self.translated_log.close()
        self.urls_log.close()
        self.url_analysis_log.close()


def translate_to_english(text):
    """Translate text to English using Google Translate API.
    Returns tuple (translation, source_language_code) or (None, None) on failure."""
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {
            'client': 'gtx',
            'sl': 'auto',
            'tl': 'en',
            'dt': 't',
            'q': text
        }
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            result = response.json()
            translated = ''.join([part[0] for part in result[0] if part[0]])
            # Source language is at index 2 of the response
            source_lang = result[2] if len(result) > 2 else 'unknown'
            return translated, source_lang
    except Exception as e:
        return None, None
    return None, None


def translate_to_finnish(text):
    """Translate text to Finnish using Google Translate API.
    Returns tuple (translation, source_language_code) or (None, None) on failure."""
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {
            'client': 'gtx',
            'sl': 'auto',
            'tl': 'fi',
            'dt': 't',
            'q': text
        }
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            result = response.json()
            translated = ''.join([part[0] for part in result[0] if part[0]])
            # Source language is at index 2 of the response
            source_lang = result[2] if len(result) > 2 else 'unknown'
            return translated, source_lang
    except Exception as e:
        return None, None
    return None, None


def is_english(text, langs=None):
    """
    Detect if text is English based on language tag or linguistic patterns.
    """
    # Check language tags first
    if langs:
        for lang in langs:
            if lang.lower().startswith('en'):
                return True

    # Fallback to pattern matching
    text_lower = text.lower()
    score = 0

    # Common English words
    english_words = ['the', 'and', 'is', 'it', 'that', 'was', 'for', 'on',
                     'are', 'with', 'they', 'be', 'at', 'one', 'have', 'this',
                     'from', 'by', 'not', 'but', 'what', 'all', 'were', 'we',
                     'when', 'your', 'can', 'said', 'there', 'use', 'an', 'each',
                     'which', 'she', 'do', 'how', 'their', 'if', 'will', 'up',
                     'about', 'out', 'many', 'then', 'them', 'these', 'so', 'some']

    words = text_lower.split()
    for word in words:
        if word in english_words:
            score += 1

    return score >= 3


def _run_gemini_sync(url):
    """Run gemini-cli synchronously (for use with asyncio.to_thread)."""
    import subprocess as sp
    try:
        prompt = f"Read this URL and create a brief summary (max 3 sentences). Describe what the page contains. URL: {url}"

        result = sp.run(
            ['gemini', '-y', prompt],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0 and result.stdout:
            return result.stdout.strip()
        else:
            return f"Analysis failed: {result.stderr.strip() if result.stderr else 'unknown error'}"
    except sp.TimeoutExpired:
        return "Analysis timed out (30s)"
    except Exception as e:
        return f"Analysis error: {str(e)}"


async def analyze_url_with_gemini(url):
    """Analyze URL content using gemini-cli and return summary."""
    try:
        return await asyncio.to_thread(_run_gemini_sync, url)
    except asyncio.CancelledError:
        return "Analysis cancelled"
    except Exception as e:
        return f"Analysis error: {str(e)}"


# Global queue for URL analysis
url_analysis_queue = None
# URL analysis enabled flag
url_analysis_enabled = False
# URL analysis statistics
url_stats = {
    'analyzed': 0,
    'skipped': 0
}


async def url_analysis_worker():
    """Worker to process URL analysis queue."""
    global quit_flag, graceful_shutdown, log_files, url_analysis_queue, url_stats

    while not quit_flag:
        try:
            # Get URL from queue with timeout
            url, timestamp = await asyncio.wait_for(
                url_analysis_queue.get(),
                timeout=0.5
            )

            # Analyze URL
            analysis = await analyze_url_with_gemini(url)

            # Log the analysis
            if log_files:
                log_files.log_url_analysis(url, analysis, timestamp)

            # Update statistics
            url_stats['analyzed'] += 1

            url_analysis_queue.task_done()

            # In graceful shutdown, show progress
            if graceful_shutdown:
                remaining = url_analysis_queue.qsize()
                if remaining > 0:
                    print(f"{BRIGHT_CYAN}>>> Links remaining: {remaining}{RESET}")
                else:
                    print(f"\n{BRIGHT_GREEN}>>> All links analyzed! Shutting down...{RESET}\n")
                    quit_flag = True
                    break

        except asyncio.TimeoutError:
            # In graceful shutdown, check if queue is empty
            if graceful_shutdown and url_analysis_queue.empty():
                print(f"\n{BRIGHT_GREEN}>>> All links analyzed! Shutting down...{RESET}\n")
                quit_flag = True
                break
            continue
        except Exception as e:
            continue


def is_bluesky_link(url):
    """Check if URL is a Bluesky internal link or profile link."""
    bluesky_domains = [
        'bsky.app',
        'bsky.social',
        'blueskyweb.xyz',
        'atproto.com'
    ]
    url_lower = url.lower()
    for domain in bluesky_domains:
        if domain in url_lower:
            return True
    return False


def display_post(post_data, keywords, english_only=False, finnish_mode=False, silent_mode=False):
    """Display a post with formatting."""
    global stats, log_files

    try:
        commit = post_data.get('commit', {})
        record = commit.get('record', {})

        text = record.get('text', '')
        langs = record.get('langs', [])
        created_at = record.get('createdAt', '')

        # Format timestamp - convert to local time
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            local_dt = dt.astimezone()  # Convert to local timezone
            time_str = local_dt.strftime('%Y-%m-%d %H:%M:%S')
            display_time = local_dt.strftime('%H:%M:%S')
        except:
            time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            display_time = datetime.now().strftime('%H:%M:%S')

        # Record all posts for statistics
        if stats:
            stats.record_post(langs)

        # Log all posts to full.log
        if log_files and text:
            log_files.log_full(text, time_str)

        if not text:
            return

        # Check if any keyword matches (case-insensitive)
        text_lower = text.lower()
        matched_keywords = [kw for kw in keywords if kw.lower() in text_lower]
        if not matched_keywords:
            return

        # Extract links from facets (exclude Bluesky internal links)
        links = []
        facets = record.get('facets', [])
        for facet in facets:
            features = facet.get('features', [])
            for feature in features:
                if feature.get('$type') == 'app.bsky.richtext.facet#link':
                    uri = feature.get('uri', '')
                    if uri and not is_bluesky_link(uri):
                        links.append(uri)

        # Extract links from embed (exclude Bluesky internal links)
        embed = record.get('embed', {})
        if embed.get('$type') == 'app.bsky.embed.external':
            external = embed.get('external', {})
            ext_uri = external.get('uri', '')
            if ext_uri and ext_uri not in links and not is_bluesky_link(ext_uri):
                links.append(ext_uri)

        # Check if post is English or Finnish
        post_is_english = is_english(text, langs)
        post_is_finnish = langs and any(l.lower().startswith('fi') for l in langs)

        # Get English translation if needed (for english_only mode or default mode)
        translation_en = None
        source_lang_en = None
        if not post_is_english and (english_only or not finnish_mode):
            translation_en, source_lang_en = translate_to_english(text)
            if translation_en and translation_en.lower() == text.lower():
                translation_en = None
                source_lang_en = None

        # Get Finnish translation if needed (for finnish_mode)
        translation_fi = None
        source_lang_fi = None
        if finnish_mode and not post_is_finnish:
            translation_fi, source_lang_fi = translate_to_finnish(text)
            if translation_fi and translation_fi.lower() == text.lower():
                translation_fi = None
                source_lang_fi = None

        # English only mode: skip posts that can't be translated
        if english_only and not post_is_english and not translation_en:
            return

        # Finnish mode: skip posts that can't be translated
        if finnish_mode and not post_is_finnish and not translation_fi:
            return

        # Record displayed post
        if stats:
            stats.record_displayed(text, keywords)

        # Log filtered post to posts.log
        if log_files:
            log_files.log_post(text, time_str)

        # Log translation to translated.log
        if log_files and (translation_en or translation_fi):
            translation = translation_fi if finnish_mode else translation_en
            if translation:
                log_files.log_translated(text, translation, time_str)

        # Log external URLs to URLs.log and queue for analysis
        if log_files:
            for link in links:
                log_files.log_url(link)
                # Add to analysis queue if enabled and queue exists
                if url_analysis_enabled and url_analysis_queue is not None:
                    try:
                        url_analysis_queue.put_nowait((link, time_str))
                    except:
                        url_stats['skipped'] += 1  # Queue full, skip analysis

        # Skip display in silent mode
        if silent_mode:
            return

        # Print separator and timestamp
        print(f"\n{BRIGHT_CYAN}{'─' * 60}{RESET}")
        print(f"{BRIGHT_YELLOW}[{display_time}]{RESET} {BRIGHT_GREEN}{', '.join(matched_keywords)}{RESET}")

        if finnish_mode:
            # Show only Finnish content
            if post_is_finnish:
                print(f"\n{BRIGHT_WHITE}{text}{RESET}")
            elif translation_fi:
                lang_name = LANGUAGE_NAMES.get(source_lang_fi, source_lang_fi) if source_lang_fi else 'Unknown'
                print(f"\n{BRIGHT_LIGHT_BLUE}[{lang_name} → FI]{RESET}")
                print(f"{BRIGHT_WHITE}{translation_fi}{RESET}")
        elif english_only:
            # Show only English content in bright white
            if post_is_english:
                print(f"\n{BRIGHT_WHITE}{text}{RESET}")
            elif translation_en:
                print(f"\n{BRIGHT_WHITE}{translation_en}{RESET}")
        else:
            # Show both original and translation
            print(f"\n{BRIGHT_WHITE}{text}{RESET}")
            if translation_en:
                lang_name = LANGUAGE_NAMES.get(source_lang_en, source_lang_en) if source_lang_en else 'Unknown'
                print(f"\n{BRIGHT_LIGHT_BLUE}[{lang_name} → EN] {translation_en}{RESET}")

        # Print external links in bright red
        for link in links:
            print(f"{BRIGHT_RED}{link}{RESET}")

    except Exception as e:
        pass  # Silently skip malformed posts


def display_live_stats():
    """Display live statistics on screen."""
    global stats

    if not stats:
        return

    # Clear screen and move cursor to top
    print("\033[2J\033[H", end="")

    duration = stats.get_duration()
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"{BRIGHT_CYAN}╔══════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BRIGHT_CYAN}║            LIVE STATISTICS  (Press Q to quit)            ║{RESET}")
    print(f"{BRIGHT_CYAN}╚══════════════════════════════════════════════════════════╝{RESET}\n")

    print(f"{BRIGHT_WHITE}Running time:{RESET} {BRIGHT_YELLOW}{hours:02d}:{minutes:02d}:{seconds:02d}{RESET}\n")

    print(f"{BRIGHT_WHITE}Posts:{RESET}")
    print(f"  Total from stream: {BRIGHT_GREEN}{stats.total_posts:,}{RESET}")
    elapsed = duration.total_seconds()
    avg_posts_per_sec = stats.total_posts / elapsed if elapsed > 0 else 0
    print(f"  Avg posts/second:  {BRIGHT_GREEN}{avg_posts_per_sec:.1f}{RESET}")
    print(f"  Filtered matches:  {BRIGHT_GREEN}{stats.displayed_posts:,}{RESET}")
    if stats.total_posts > 0:
        percentage = (stats.displayed_posts / stats.total_posts) * 100
        print(f"  Match rate:        {BRIGHT_GREEN}{percentage:.4f}%{RESET}")

    print(f"\n{BRIGHT_WHITE}Keyword matches:{RESET}")
    if stats.keyword_counts:
        # Sort keywords by match count (highest first)
        sorted_keywords = sorted(stats.keywords, key=lambda kw: stats.keyword_counts.get(kw, 0), reverse=True)
        for kw in sorted_keywords:
            count = stats.keyword_counts.get(kw, 0)
            print(f"  {BRIGHT_YELLOW}{kw}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
    else:
        print(f"  {BRIGHT_YELLOW}No matches yet{RESET}")

    print(f"\n{BRIGHT_WHITE}Top 10 Languages:{RESET}")
    if stats.language_counts:
        sorted_langs = sorted(stats.language_counts.items(), key=lambda x: x[1], reverse=True)
        for lang_code, count in sorted_langs[:10]:
            percentage = (count / stats.total_posts) * 100 if stats.total_posts > 0 else 0
            lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
            print(f"  {BRIGHT_MAGENTA}{lang_name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} ({percentage:.1f}%)")
    else:
        print(f"  {BRIGHT_YELLOW}No data yet{RESET}")

    # URL analysis statistics (only if enabled)
    if url_analysis_enabled:
        print(f"\n{BRIGHT_WHITE}URL analysis (gemini-cli):{RESET}")
        queue_size = url_analysis_queue.qsize() if url_analysis_queue else 0
        print(f"  Analyzed:  {BRIGHT_GREEN}{url_stats['analyzed']:,}{RESET}")
        print(f"  Queued:    {BRIGHT_YELLOW}{queue_size:,}{RESET}")
        print(f"  Skipped:   {BRIGHT_RED}{url_stats['skipped']:,}{RESET}")


async def check_keyboard():
    """Check for keyboard input (Q to quit)."""
    global quit_flag, graceful_shutdown

    while not quit_flag:
        # Non-blocking check using select with very short timeout
        ready, _, _ = select.select([sys.stdin], [], [], 0.05)
        if ready:
            try:
                char = sys.stdin.read(1)
                if char.lower() == 'q':
                    if not graceful_shutdown and url_analysis_enabled:
                        # First Q: stop fetching posts, continue URL analysis
                        graceful_shutdown = True
                        queue_size = url_analysis_queue.qsize() if url_analysis_queue else 0
                        print(f"\n\n{BRIGHT_YELLOW}>>> Post fetching stopped.{RESET}")
                        if queue_size > 0:
                            print(f"{BRIGHT_CYAN}>>> Continuing {queue_size} link analysis... (Q = stop all){RESET}\n")
                        else:
                            print(f"{BRIGHT_CYAN}>>> No links in queue, shutting down...{RESET}\n")
                            quit_flag = True
                            break
                    else:
                        # Immediate quit (no URL analysis or second Q)
                        if graceful_shutdown:
                            print(f"\n\n{BRIGHT_RED}>>> Stopping all, shutting down...{RESET}\n")
                        else:
                            print(f"\n\n{BRIGHT_YELLOW}>>> Shutting down...{RESET}\n")
                        quit_flag = True
                        break
            except:
                pass

        # Yield to event loop
        await asyncio.sleep(0)


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


async def update_live_stats(interval=1):
    """Update live statistics display periodically."""
    global quit_flag

    while not quit_flag:
        display_live_stats()
        await asyncio.sleep(interval)


async def monitor_jetstream(keywords, english_only=False, finnish_mode=False, silent_mode=False):
    """Connect to Jetstream and monitor for posts containing keywords."""
    global quit_flag, graceful_shutdown

    if not silent_mode:
        print(f"\n{BRIGHT_WHITE}Connecting to Bluesky Jetstream...{RESET}")
        print(f"{BRIGHT_YELLOW}Monitoring for keywords: {', '.join(keywords)}{RESET}")
        if english_only:
            print(f"{BRIGHT_CYAN}Mode: English only{RESET}")
        elif finnish_mode:
            print(f"{BRIGHT_CYAN}Mode: Finnish only (translated){RESET}")
        print(f"{BRIGHT_CYAN}Press Q to quit{RESET}\n")

    while not quit_flag and not graceful_shutdown:
        try:
            async with websockets.connect(JETSTREAM_URL, ping_interval=30, ping_timeout=10) as websocket:
                if not silent_mode:
                    print(f"{BRIGHT_WHITE}Connected! Waiting for posts...{RESET}")

                # Start ping task
                ping_task = asyncio.create_task(websocket_ping(websocket, 30))

                try:
                    while not quit_flag and not graceful_shutdown and not check_run_limits():
                        try:
                            # Wait for message with timeout to check quit flag
                            message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                            data = json.loads(message)

                            # Only process commit messages for posts
                            if data.get('kind') == 'commit':
                                commit = data.get('commit', {})
                                if commit.get('operation') == 'create':
                                    if commit.get('collection') == 'app.bsky.feed.post':
                                        display_post(data, keywords, english_only, finnish_mode, silent_mode)
                                        # Check limits after each displayed post
                                        check_run_limits()

                        except asyncio.TimeoutError:
                            # Check limits on timeout too
                            check_run_limits()
                            continue
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            continue
                finally:
                    # Cancel ping task when done
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosed:
            if not quit_flag and not graceful_shutdown and not silent_mode:
                print(f"\n{BRIGHT_YELLOW}Connection closed. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)
        except Exception as e:
            if not quit_flag and not graceful_shutdown and not silent_mode:
                print(f"\n{BRIGHT_YELLOW}Connection error: {e}. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)


async def run_monitor(keywords, english_only=False, finnish_mode=False, silent_mode=False):
    """Run the monitor with keyboard detection."""
    global quit_flag, graceful_shutdown, url_analysis_queue, url_analysis_enabled

    # Initialize URL analysis queue only if enabled
    if url_analysis_enabled:
        url_analysis_queue = asyncio.Queue(maxsize=100)

    # Create tasks for monitoring and keyboard checking
    monitor_task = asyncio.create_task(monitor_jetstream(keywords, english_only, finnish_mode, silent_mode))
    keyboard_task = asyncio.create_task(check_keyboard())

    tasks = [monitor_task, keyboard_task]

    # Add URL worker task only if enabled
    url_worker_task = None
    if url_analysis_enabled:
        url_worker_task = asyncio.create_task(url_analysis_worker())
        tasks.append(url_worker_task)

    # Add live stats task if in silent mode
    if silent_mode:
        stats_task = asyncio.create_task(update_live_stats(1))
        tasks.append(stats_task)

    # Wait for quit_flag to be set (either immediate quit or after graceful shutdown completes)
    while not quit_flag:
        done, pending = await asyncio.wait(
            [t for t in tasks if not t.done()],
            timeout=0.5,
            return_when=asyncio.FIRST_COMPLETED
        )
        # If in graceful shutdown mode, monitor task may have completed
        # Keep running keyboard and url_worker tasks
        if graceful_shutdown and not quit_flag and url_analysis_enabled:
            # Check if url_worker finished (queue empty)
            if url_worker_task and url_worker_task.done():
                quit_flag = True
                break

    # Cancel pending tasks
    for task in tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


def load_keywords_from_file():
    """Load keywords from keywords.txt if it exists."""
    if os.path.exists('keywords.txt'):
        with open('keywords.txt', 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
            return keywords
    return []


def save_keywords_to_file(keywords):
    """Save keywords to keywords.txt."""
    with open('keywords.txt', 'w', encoding='utf-8') as f:
        for keyword in keywords:
            f.write(f"{keyword}\n")


def main():
    """Main entry point."""
    global stats, quit_flag, graceful_shutdown, log_files

    print(f"\n{BRIGHT_CYAN}╔════════════════════════════════════════╗{RESET}")
    print(f"{BRIGHT_CYAN}║  Bluesky Jetstream Keyword Monitor     ║{RESET}")
    print(f"{BRIGHT_CYAN}╚════════════════════════════════════════╝{RESET}\n")

    # Check for saved keywords
    keywords = []
    saved_keywords = load_keywords_from_file()

    if saved_keywords:
        print(f"{BRIGHT_CYAN}Saved keywords found:{RESET}")
        for i, kw in enumerate(saved_keywords, 1):
            print(f"  {BRIGHT_WHITE}{i}. {kw}{RESET}")

        use_saved = input(f"\n{BRIGHT_CYAN}Use saved keywords? [Y/n]: {RESET}").strip().lower()

        if use_saved != 'n':
            keywords = saved_keywords
            print(f"\n{BRIGHT_GREEN}Using saved keywords.{RESET}")
        else:
            print(f"\n{BRIGHT_CYAN}Enter new keywords to monitor (empty line to finish):{RESET}")

    if not keywords:
        # Collect new keywords
        if not saved_keywords:
            print(f"{BRIGHT_CYAN}Enter keywords to monitor (empty line to finish):{RESET}")

        while True:
            prompt = f"  Keyword {len(keywords) + 1}: "
            keyword = input(prompt).strip()

            if not keyword:
                break
            keywords.append(keyword)

        if not keywords:
            print("No keywords provided. Exiting.")
            return

        # Save new keywords to file
        save_keywords_to_file(keywords)
        print(f"{BRIGHT_GREEN}Keywords saved to keywords.txt{RESET}")

    print(f"\n{BRIGHT_WHITE}Keywords: {', '.join(keywords)}{RESET}")

    # Ask for run mode
    print(f"\n{BRIGHT_CYAN}Run mode:{RESET}")
    print(f"  {BRIGHT_WHITE}1{RESET} - Run until interrupted (default)")
    print(f"  {BRIGHT_WHITE}2{RESET} - Run for specific duration")
    print(f"  {BRIGHT_WHITE}3{RESET} - Run until specific number of filtered posts")
    run_mode = input("\nSelect run mode [1]: ").strip()

    global run_time_limit, run_post_limit
    run_time_limit = None
    run_post_limit = None

    if run_mode == "2":
        print(f"\n{BRIGHT_CYAN}Enter duration:{RESET}")
        try:
            hours = int(input("  Hours [0]: ").strip() or "0")
            minutes = int(input("  Minutes [0]: ").strip() or "0")
            seconds = int(input("  Seconds [0]: ").strip() or "0")
            run_time_limit = hours * 3600 + minutes * 60 + seconds
            if run_time_limit <= 0:
                print(f"{BRIGHT_YELLOW}Invalid duration. Running until interrupted.{RESET}")
                run_time_limit = None
            else:
                print(f"{BRIGHT_GREEN}Collection cycle: {hours:02d}:{minutes:02d}:{seconds:02d} (auto-archives and continues){RESET}")
        except ValueError:
            print(f"{BRIGHT_YELLOW}Invalid input. Running until interrupted.{RESET}")
            run_time_limit = None

    elif run_mode == "3":
        try:
            post_count = int(input(f"\n{BRIGHT_CYAN}Number of filtered posts to collect: {RESET}").strip())
            if post_count <= 0:
                print(f"{BRIGHT_YELLOW}Invalid number. Running until interrupted.{RESET}")
                run_post_limit = None
            else:
                run_post_limit = post_count
                print(f"{BRIGHT_GREEN}Will collect {post_count:,} filtered posts{RESET}")
        except ValueError:
            print(f"{BRIGHT_YELLOW}Invalid input. Running until interrupted.{RESET}")
            run_post_limit = None

    # Ask for display mode
    print(f"\n{BRIGHT_CYAN}Display mode:{RESET}")
    print(f"  {BRIGHT_WHITE}1{RESET} - Show original + English translation (default)")
    print(f"  {BRIGHT_WHITE}2{RESET} - Show only English (translated)")
    print(f"  {BRIGHT_WHITE}3{RESET} - Show only Finnish (translated)")
    print(f"  {BRIGHT_WHITE}4{RESET} - Background mode (live statistics only)")
    mode_choice = input("\nSelect mode [1]: ").strip()

    english_only = mode_choice == "2"
    finnish_mode = mode_choice == "3"
    silent_mode = mode_choice == "4"

    # Ask about URL analysis
    global url_analysis_enabled
    print(f"\n{BRIGHT_CYAN}URL analysis:{RESET}")
    print(f"  Analyze external links with gemini-cli in background")
    url_analysis_choice = input(f"Enable URL analysis? [y/N]: ").strip().lower()
    url_analysis_enabled = url_analysis_choice == 'y'

    # Initialize statistics and log files
    stats = Statistics(keywords)
    log_files = LogFiles()
    quit_flag = False
    graceful_shutdown = False

    if url_analysis_enabled:
        print(f"\n{BRIGHT_CYAN}Logging to: full.log, posts.log, translated.log, URLs.log, url-analysis.log{RESET}")
        print(f"{BRIGHT_CYAN}URL analysis running with gemini-cli in background{RESET}")
    else:
        print(f"\n{BRIGHT_CYAN}Logging to: full.log, posts.log, translated.log, URLs.log{RESET}")

    # Save terminal settings
    old_settings = termios.tcgetattr(sys.stdin)

    try:
        # Set terminal to raw mode for single character input
        tty.setcbreak(sys.stdin.fileno())

        asyncio.run(run_monitor(keywords, english_only, finnish_mode, silent_mode))

    except KeyboardInterrupt:
        pass
    finally:
        # Restore terminal settings
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

        # Close log files
        if log_files:
            log_files.close()

        # Finish statistics and print report
        stats.finish()
        stats.print_report()


if __name__ == "__main__":
    main()
