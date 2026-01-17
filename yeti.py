#!/usr/bin/env python3
"""
Yeti - Bluesky Jetstream Monitor
Real-time post monitoring with keyword filtering and batch translation.
Connects to Bluesky Jetstream WebSocket service and displays posts matching keywords.
Posts are translated to English and Finnish after monitoring ends.
"""

import json
import asyncio
import glob
import os
import select
import shutil
import sys
import termios
import tty
from datetime import datetime
from collections import defaultdict

import requests
import websockets

# ANSI color codes
RESET = "\033[0m"
BRIGHT_CYAN = "\033[1;36m"
BRIGHT_YELLOW = "\033[1;93m"
BRIGHT_WHITE = "\033[1;97m"
BRIGHT_RED = "\033[1;91m"
BRIGHT_GREEN = "\033[1;92m"
BRIGHT_MAGENTA = "\033[1;95m"
BRIGHT_LIGHT_BLUE = "\033[1;96m"

# Jetstream WebSocket endpoint
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

# Bluesky domains for filtering internal links
BLUESKY_DOMAINS = frozenset(['bsky.app', 'bsky.social', 'blueskyweb.xyz', 'atproto.com'])

# Common English words for language detection (frozen set for O(1) lookup)
ENGLISH_WORDS = frozenset([
    'the', 'and', 'is', 'it', 'that', 'was', 'for', 'on', 'are', 'with',
    'they', 'be', 'at', 'one', 'have', 'this', 'from', 'by', 'not', 'but',
    'what', 'all', 'were', 'we', 'when', 'your', 'can', 'said', 'there',
    'use', 'an', 'each', 'which', 'she', 'do', 'how', 'their', 'if', 'will',
    'up', 'about', 'out', 'many', 'then', 'them', 'these', 'so', 'some'
])

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

# Log file patterns for backup/archive
LOG_PATTERNS = [
    'ALL.*.log', 'FILTER.*.log', 'ENG_*.log', 'FIN_*.log',
    'EXT-URL.*.log', 'PROFILES.*.log', 'FILTER_hashtag.*.log',
    'ALL_hashtag.*.log', 'REPORT_*.log'
]

# Global state
stats = None
quit_flag = False
log_files = None
run_time_limit = None
run_post_limit = None
filtered_posts_for_translation = []
http_session = None  # Reusable HTTP session


def get_http_session():
    """Get or create a reusable HTTP session."""
    global http_session
    if http_session is None:
        http_session = requests.Session()
        http_session.headers.update({'User-Agent': 'Yeti/1.0'})
    return http_session


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
        self.unique_profiles = set()
        self.profile_post_counts = defaultdict(int)
        self.profile_handles = {}
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

    def record_profile(self, profile_url, handle=None, display_name=None):
        """Record unique profile and count posts per profile."""
        is_new = profile_url not in self.unique_profiles
        self.unique_profiles.add(profile_url)
        self.profile_post_counts[profile_url] += 1
        if profile_url not in self.profile_handles:
            self.profile_handles[profile_url] = display_name or handle or ''
        return is_new

    def record_url(self):
        """Record URL count."""
        self.total_urls += 1

    def finish(self):
        """Mark the session as finished."""
        self.end_time = datetime.now()

    def get_duration(self):
        """Get the duration of the session."""
        return (self.end_time or datetime.now()) - self.start_time

    def print_report(self, report_filename=None):
        """Print the statistics report and save to file."""
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
            f"  Unique profiles:          {len(self.unique_profiles):,}",
            f"  External URLs:            {self.total_urls:,}",
            f"  Hashtags (filtered):      {sum(self.hashtag_counts.values()):,}",
            f"  Unique hashtags (filter): {len(self.hashtag_counts):,}", "",
        ])

        # Top 5 sections
        self._add_top_section(lines, "TOP 5 KEYWORD MATCHES:", self.keyword_counts, self.keywords)
        self._add_top_authors(lines)
        self._add_top_langs(lines)
        self._add_top_hashtags(lines, "TOP 5 HASHTAGS (filtered):", self.hashtag_counts)
        self._add_top_hashtags(lines, "TOP 5 HASHTAGS (all stream):", self.all_hashtag_counts)

        lines.extend(["", "=" * 70])

        # Save to file
        filename = report_filename or 'REPORT.log'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        # Print colored version
        self._print_colored_report(hours, minutes, seconds, posts_per_sec, filtered_per_min, filename)

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

    def _add_top_authors(self, lines):
        """Add top 5 authors section."""
        lines.append("TOP 5 AUTHORS (filtered posts):")
        if self.profile_post_counts:
            sorted_profiles = sorted(self.profile_post_counts.items(), key=lambda x: x[1], reverse=True)
            for url, count in sorted_profiles[:5]:
                name = self.profile_handles.get(url, '')
                lines.extend([f"  {name}: {count:,} posts", f"    {url}"])
            if len(sorted_profiles) > 5:
                lines.append(f"  ... and {len(sorted_profiles) - 5} more authors")
        else:
            lines.append("  No authors")
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

    def _print_colored_report(self, hours, minutes, seconds, posts_per_sec, filtered_per_min, filename):
        """Print colored version of report to terminal."""
        print(f"\n\n{BRIGHT_CYAN}{'═' * 70}{RESET}")
        print(f"{BRIGHT_CYAN}                         SESSION REPORT{RESET}")
        print(f"{BRIGHT_CYAN}{'═' * 70}{RESET}\n")

        print(f"{BRIGHT_WHITE}TIME:{RESET}")
        print(f"  Started:  {BRIGHT_YELLOW}{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Ended:    {BRIGHT_YELLOW}{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Duration: {BRIGHT_YELLOW}{hours:02d}:{minutes:02d}:{seconds:02d}{RESET}")

        print(f"\n{BRIGHT_WHITE}STREAM STATISTICS:{RESET}")
        print(f"  Total posts processed:    {BRIGHT_GREEN}{self.total_posts:,}{RESET}")
        print(f"  Average posts/second:     {BRIGHT_GREEN}{posts_per_sec:.1f}{RESET}")
        print(f"  Total hashtags (stream):  {BRIGHT_GREEN}{sum(self.all_hashtag_counts.values()):,}{RESET}")
        print(f"  Unique hashtags (stream): {BRIGHT_GREEN}{len(self.all_hashtag_counts):,}{RESET}")

        print(f"\n{BRIGHT_WHITE}FILTERED STATISTICS:{RESET}")
        print(f"  Filtered posts:           {BRIGHT_GREEN}{self.displayed_posts:,}{RESET}")
        if self.total_posts > 0:
            pct = (self.displayed_posts / self.total_posts) * 100
            print(f"  Match rate:               {BRIGHT_GREEN}{pct:.4f}%{RESET}")
        print(f"  Filtered posts/minute:    {BRIGHT_GREEN}{filtered_per_min:.1f}{RESET}")
        print(f"  Unique profiles:          {BRIGHT_GREEN}{len(self.unique_profiles):,}{RESET}")
        print(f"  External URLs:            {BRIGHT_GREEN}{self.total_urls:,}{RESET}")

        self._print_top_keywords()
        self._print_top_authors()
        self._print_top_langs()
        self._print_top_hashtags()

        print(f"\n{BRIGHT_CYAN}{'═' * 70}{RESET}")
        print(f"{BRIGHT_GREEN}Report saved to {filename}{RESET}\n")

    def _print_top_keywords(self):
        """Print top 5 keywords with colors."""
        print(f"\n{BRIGHT_WHITE}TOP 5 KEYWORD MATCHES:{RESET}")
        if self.keyword_counts:
            sorted_kw = sorted([k for k in self.keywords if self.keyword_counts.get(k, 0) > 0],
                              key=lambda x: self.keyword_counts.get(x, 0), reverse=True)
            for kw in sorted_kw[:5]:
                print(f"  {BRIGHT_YELLOW}{kw}{RESET}: {BRIGHT_GREEN}{self.keyword_counts[kw]:,}{RESET}")
            if len(sorted_kw) > 5:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_kw) - 5} more keywords{RESET}")
            if not sorted_kw:
                print(f"  {BRIGHT_YELLOW}No matches{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No matches{RESET}")

    def _print_top_authors(self):
        """Print top 5 authors with colors."""
        print(f"\n{BRIGHT_WHITE}TOP 5 AUTHORS (filtered):{RESET}")
        if self.profile_post_counts:
            sorted_p = sorted(self.profile_post_counts.items(), key=lambda x: x[1], reverse=True)
            for url, count in sorted_p[:5]:
                name = self.profile_handles.get(url, '')
                print(f"  {BRIGHT_CYAN}{name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} posts")
                print(f"    {BRIGHT_WHITE}{url}{RESET}")
            if len(sorted_p) > 5:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_p) - 5} more authors{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No authors{RESET}")

    def _print_top_langs(self):
        """Print top 5 languages with colors."""
        print(f"\n{BRIGHT_WHITE}TOP 5 LANGUAGES (filtered):{RESET}")
        if self.language_counts:
            sorted_l = sorted(self.language_counts.items(), key=lambda x: x[1], reverse=True)
            for code, count in sorted_l[:5]:
                pct = (count / self.displayed_posts * 100) if self.displayed_posts > 0 else 0
                name = LANGUAGE_NAMES.get(code, code)
                print(f"  {BRIGHT_MAGENTA}{name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} ({pct:.1f}%)")
            if len(sorted_l) > 5:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_l) - 5} more languages{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No language data{RESET}")

    def _print_top_hashtags(self):
        """Print top 5 hashtags with colors."""
        print(f"\n{BRIGHT_WHITE}TOP 5 HASHTAGS (filtered):{RESET}")
        if self.hashtag_counts:
            sorted_h = sorted(self.hashtag_counts.items(), key=lambda x: x[1], reverse=True)
            for tag, count in sorted_h[:5]:
                print(f"  {BRIGHT_CYAN}#{tag}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
            if len(sorted_h) > 5:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_h) - 5} more hashtags{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No hashtags{RESET}")


class LogFiles:
    """Handle logging to multiple files with timestamps."""

    def __init__(self, skip_backup=False):
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.seen_profiles = set()

        if not skip_backup:
            self._backup_existing_logs()

        # Open all log files
        self.all_log = open(f'ALL.{self.timestamp}.log', 'w', encoding='utf-8')
        self.filter_log = open(f'FILTER.{self.timestamp}.log', 'w', encoding='utf-8')
        self.eng_log = open(f'ENG_{self.timestamp}.log', 'w', encoding='utf-8')
        self.fin_log = open(f'FIN_{self.timestamp}.log', 'w', encoding='utf-8')
        self.url_log = open(f'EXT-URL.{self.timestamp}.log', 'w', encoding='utf-8')
        self.profiles_log = open(f'PROFILES.{self.timestamp}.log', 'w', encoding='utf-8')
        self.filter_hashtag_log = open(f'FILTER_hashtag.{self.timestamp}.log', 'w', encoding='utf-8')
        self.all_hashtag_log = open(f'ALL_hashtag.{self.timestamp}.log', 'w', encoding='utf-8')
        self.report_filename = f'REPORT_{self.timestamp}.log'

    def _backup_existing_logs(self):
        """Backup existing log files to a timestamped directory."""
        existing = []
        for pattern in LOG_PATTERNS:
            existing.extend(glob.glob(pattern))

        if not existing:
            return

        backup_dir = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_logs"
        os.makedirs(backup_dir, exist_ok=True)

        for f in existing:
            shutil.move(f, os.path.join(backup_dir, f))

        print(f"{BRIGHT_YELLOW}Existing logs moved to: {backup_dir}/{RESET}")

    def log_all(self, text, timestamp):
        """Log all posts from stream."""
        self.all_log.write(f"[{timestamp}] {text}\n")

    def log_filter(self, text, timestamp, matched_keywords, post_url=None, handle=None):
        """Log filtered posts with full details."""
        self.filter_log.write(f"{'=' * 60}\n[{timestamp}]\n")
        if handle:
            self.filter_log.write(f"User: {handle}\n")
        self.filter_log.write(f"Keywords: {', '.join(matched_keywords)}\nText: {text}\n")
        if post_url:
            self.filter_log.write(f"URL: {post_url}\n")
        self.filter_log.write("\n")

    def log_translation(self, log_file, original, translation, timestamp, source_lang,
                        author_name, author_handle, post_url, target_lang):
        """Log translation with full details."""
        lang_name = LANGUAGE_NAMES.get(source_lang, source_lang) if source_lang else 'Unknown'
        log_file.write(f"{'═' * 60}\n[{timestamp}] [{lang_name}]\n")
        if author_name or author_handle:
            log_file.write(f"Author: {author_name or author_handle}\n")
        if post_url:
            log_file.write(f"Post: {post_url}\n")
        log_file.write(f"\nORIGINAL:\n{original}\n\n{target_lang}:\n{translation}\n\n")

    def log_eng(self, original, translation, timestamp, source_lang=None,
                author_name=None, author_handle=None, post_url=None):
        """Log English translation."""
        self.log_translation(self.eng_log, original, translation, timestamp,
                           source_lang, author_name, author_handle, post_url, "ENGLISH")

    def log_fin(self, original, translation, timestamp, source_lang=None,
                author_name=None, author_handle=None, post_url=None):
        """Log Finnish translation."""
        self.log_translation(self.fin_log, original, translation, timestamp,
                           source_lang, author_name, author_handle, post_url, "FINNISH")

    def log_url(self, url):
        """Log external URL."""
        self.url_log.write(f"{url}\n")

    def log_profile(self, profile_url, display_name=None):
        """Log unique profile URL with display name."""
        if profile_url not in self.seen_profiles:
            self.seen_profiles.add(profile_url)
            if display_name:
                self.profiles_log.write(f"{profile_url} | {display_name}\n")
            else:
                self.profiles_log.write(f"{profile_url}\n")

    def log_filter_hashtag(self, hashtag):
        """Log hashtag from filtered posts."""
        self.filter_hashtag_log.write(f"{hashtag}\n")

    def log_all_hashtag(self, hashtag):
        """Log hashtag from all posts."""
        self.all_hashtag_log.write(f"{hashtag}\n")

    def flush_all(self):
        """Flush all log files."""
        for log in [self.all_log, self.filter_log, self.eng_log, self.fin_log,
                   self.url_log, self.profiles_log, self.filter_hashtag_log, self.all_hashtag_log]:
            log.flush()

    def get_report_filename(self):
        """Get the report filename."""
        return self.report_filename

    def close(self):
        """Close all log files."""
        for log in [self.all_log, self.filter_log, self.eng_log, self.fin_log,
                   self.url_log, self.profiles_log, self.filter_hashtag_log, self.all_hashtag_log]:
            log.close()


def fetch_bluesky_profile(did):
    """Fetch Bluesky profile info for a DID."""
    try:
        session = get_http_session()
        resp = session.get(
            "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile",
            params={'actor': did},
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            return {'displayName': data.get('displayName', ''), 'handle': data.get('handle', '')}
    except Exception:
        pass
    return None


def fetch_profiles_batch(dids, progress_callback=None):
    """Fetch display names for multiple DIDs."""
    profiles = {}
    total = len(dids)
    for i, did in enumerate(dids, 1):
        if progress_callback:
            progress_callback(i, total)
        profile = fetch_bluesky_profile(did)
        if profile:
            profiles[did] = profile
    return profiles


def translate_text(text, target_lang):
    """Translate text using Google Translate API.
    Returns tuple (translation, source_language_code) or (None, None) on failure."""
    try:
        session = get_http_session()
        resp = session.get(
            "https://translate.googleapis.com/translate_a/single",
            params={'client': 'gtx', 'sl': 'auto', 'tl': target_lang, 'dt': 't', 'q': text},
            timeout=5
        )
        if resp.status_code == 200:
            result = resp.json()
            translated = ''.join(part[0] for part in result[0] if part[0])
            source_lang = result[2] if len(result) > 2 else 'unknown'
            return translated, source_lang
    except Exception:
        pass
    return None, None


def translate_to_english(text):
    """Translate text to English."""
    return translate_text(text, 'en')


def translate_to_finnish(text):
    """Translate text to Finnish."""
    return translate_text(text, 'fi')


def is_bluesky_link(url):
    """Check if URL is a Bluesky internal link."""
    url_lower = url.lower()
    return any(domain in url_lower for domain in BLUESKY_DOMAINS)


def extract_hashtags(record):
    """Extract hashtags from post facets."""
    hashtags = []
    for facet in record.get('facets', []):
        for feature in facet.get('features', []):
            if feature.get('$type') == 'app.bsky.richtext.facet#tag':
                tag = feature.get('tag', '')
                if tag and tag not in hashtags:
                    hashtags.append(tag)
    return hashtags


def archive_logs_and_reset():
    """Archive current logs and reset for new collection cycle."""
    global stats, log_files, filtered_posts_for_translation

    batch_translate_posts()
    stats.finish()

    if log_files:
        stats.print_report(log_files.get_report_filename())
        log_files.close()

    # Create archive directory
    archive_dir = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_logs_archive"
    os.makedirs(archive_dir, exist_ok=True)

    for pattern in LOG_PATTERNS:
        for f in glob.glob(pattern):
            shutil.move(f, os.path.join(archive_dir, f))

    print(f"\n{BRIGHT_GREEN}>>> Logs archived to: {archive_dir}/{RESET}")
    print(f"{BRIGHT_CYAN}>>> Starting new collection cycle...{RESET}\n")

    # Reset
    keywords = stats.keywords
    stats = Statistics(keywords)
    filtered_posts_for_translation = []
    log_files = LogFiles(skip_backup=True)


def check_run_limits():
    """Check if run limits have been reached."""
    global quit_flag, stats, run_time_limit, run_post_limit

    if quit_flag:
        return True

    if stats:
        if run_time_limit is not None:
            if stats.get_duration().total_seconds() >= run_time_limit:
                print(f"\n\n{BRIGHT_YELLOW}>>> Time limit reached. Archiving logs...{RESET}")
                archive_logs_and_reset()
                return False

        if run_post_limit is not None:
            if stats.displayed_posts >= run_post_limit:
                print(f"\n\n{BRIGHT_YELLOW}>>> Post limit reached ({run_post_limit:,} posts). Shutting down...{RESET}\n")
                quit_flag = True
                return True

    return False


def batch_translate_posts():
    """Translate all filtered posts to English and Finnish."""
    global filtered_posts_for_translation, log_files, stats

    total = len(filtered_posts_for_translation)
    if total == 0:
        print(f"\n{BRIGHT_YELLOW}No posts to translate.{RESET}")
        return

    unique_dids = list(set(p['did'] for p in filtered_posts_for_translation if p.get('did')))

    print(f"\n{BRIGHT_CYAN}{'═' * 60}{RESET}")
    print(f"{BRIGHT_WHITE}TRANSLATION PHASE{RESET}")
    print(f"{BRIGHT_CYAN}{'═' * 60}{RESET}")
    print(f"\n{BRIGHT_WHITE}Posts to translate:{RESET} {BRIGHT_GREEN}{total:,}{RESET}")
    print(f"{BRIGHT_WHITE}Unique profiles:{RESET} {BRIGHT_GREEN}{len(unique_dids):,}{RESET}")

    # 10-second countdown with Q to cancel
    print(f"\n{BRIGHT_YELLOW}Starting translation in 10 seconds... Press Q to cancel{RESET}")

    old_settings = termios.tcgetattr(sys.stdin)
    cancelled = False
    try:
        tty.setcbreak(sys.stdin.fileno())
        for countdown in range(10, 0, -1):
            print(f"\r{BRIGHT_CYAN}Starting in {countdown}...{RESET}  ", end="", flush=True)
            ready, _, _ = select.select([sys.stdin], [], [], 1.0)
            if ready:
                if sys.stdin.read(1).lower() == 'q':
                    print(f"\r{BRIGHT_RED}Translation cancelled by user.{RESET}          ")
                    cancelled = True
                    break
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

    if cancelled:
        return

    print(f"\r{BRIGHT_GREEN}Starting translation...{RESET}      ")

    # Fetch profiles
    print(f"\n{BRIGHT_WHITE}Fetching profile display names...{RESET}")
    profile_cache = fetch_profiles_batch(
        unique_dids,
        lambda c, t: print(f"\r{BRIGHT_YELLOW}Fetching profiles: {c}/{t} ({c*100//t}%){RESET}  ", end="", flush=True)
    )
    print(f"\r{BRIGHT_GREEN}Fetched {len(profile_cache)} profile names.{RESET}              ")

    # Update PROFILES log
    if log_files:
        print(f"{BRIGHT_WHITE}Updating PROFILES log...{RESET}")
        log_files.profiles_log.seek(0)
        log_files.profiles_log.truncate()
        log_files.seen_profiles.clear()
        for did in unique_dids:
            url = f"https://bsky.app/profile/{did}"
            info = profile_cache.get(did, {})
            log_files.log_profile(url, info.get('displayName', ''))

    # Update stats
    if stats:
        for did in unique_dids:
            url = f"https://bsky.app/profile/{did}"
            info = profile_cache.get(did, {})
            name = info.get('displayName') or (f"@{info.get('handle')}" if info.get('handle') else '')
            if name:
                stats.profile_handles[url] = name

    # Translate
    print(f"\n{BRIGHT_WHITE}Translating posts...{RESET}")
    translated_en, translated_fi, skipped_en, skipped_fi = 0, 0, 0, 0

    for i, post in enumerate(filtered_posts_for_translation, 1):
        text, ts, lang = post['text'], post['timestamp'], post['lang']
        did, post_url = post.get('did', ''), post.get('post_url', '')
        info = profile_cache.get(did, {})
        author_name, author_handle = info.get('displayName', ''), info.get('handle', '')

        print(f"\r{BRIGHT_YELLOW}Progress: {i:,}/{total:,} ({i*100//total}%) - EN: {translated_en:,} FI: {translated_fi:,}{RESET}", end="", flush=True)

        is_en = lang and lang.lower().startswith('en')
        is_fi = lang and lang.lower().startswith('fi')

        # English translation
        if is_en:
            skipped_en += 1
            if log_files:
                log_files.log_eng(text, text, ts, lang, author_name, author_handle, post_url)
        else:
            trans, src = translate_to_english(text)
            if trans and trans.lower() != text.lower():
                translated_en += 1
            if log_files:
                log_files.log_eng(text, trans or text, ts, src or lang, author_name, author_handle, post_url)

        # Finnish translation
        if is_fi:
            skipped_fi += 1
            if log_files:
                log_files.log_fin(text, text, ts, lang, author_name, author_handle, post_url)
        else:
            trans, src = translate_to_finnish(text)
            if trans and trans.lower() != text.lower():
                translated_fi += 1
            if log_files:
                log_files.log_fin(text, trans or text, ts, src or lang, author_name, author_handle, post_url)

    print(f"\r{' ' * 80}\r", end="")
    print(f"{BRIGHT_GREEN}Translation complete!{RESET}")
    print(f"  {BRIGHT_WHITE}English:{RESET} {translated_en:,} translated, {skipped_en:,} already English")
    print(f"  {BRIGHT_WHITE}Finnish:{RESET} {translated_fi:,} translated, {skipped_fi:,} already Finnish")
    print(f"{BRIGHT_CYAN}{'═' * 60}{RESET}\n")


def display_post(post_data, keywords, keywords_lower, silent_mode=False):
    """Display and process a post."""
    global stats, log_files, filtered_posts_for_translation

    try:
        commit = post_data.get('commit', {})
        record = commit.get('record', {})
        did = post_data.get('did', '')
        rkey = commit.get('rkey', '')
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

        profile_url = f"https://bsky.app/profile/{did}" if did else None
        post_url = f"https://bsky.app/profile/{did}/post/{rkey}" if did and rkey else None
        handle = did.replace('did:plc:', '@') if did else '@unknown'

        if stats:
            stats.record_post()

        # Extract and log hashtags from ALL posts
        hashtags = extract_hashtags(record)
        if hashtags:
            if stats:
                stats.record_all_hashtags(hashtags)
            if log_files:
                for tag in hashtags:
                    log_files.log_all_hashtag(tag)

        if log_files and text:
            log_files.log_all(text, time_str)

        if not text:
            return

        # Check keyword matches (pre-computed lowercase)
        text_lower = text.lower()
        matched = [kw for kw, kw_low in zip(keywords, keywords_lower) if kw_low in text_lower]
        if not matched:
            return

        if stats:
            stats.record_language(langs)

        if hashtags:
            if stats:
                stats.record_hashtags(hashtags)
            if log_files:
                for tag in hashtags:
                    log_files.log_filter_hashtag(tag)

        # Extract external links
        links = []
        for facet in record.get('facets', []):
            for feature in facet.get('features', []):
                if feature.get('$type') == 'app.bsky.richtext.facet#link':
                    uri = feature.get('uri', '')
                    if uri and not is_bluesky_link(uri):
                        links.append(uri)

        embed = record.get('embed', {})
        if embed.get('$type') == 'app.bsky.embed.external':
            uri = embed.get('external', {}).get('uri', '')
            if uri and uri not in links and not is_bluesky_link(uri):
                links.append(uri)

        # Store for batch translation
        lang_code = langs[0] if langs else 'unknown'
        filtered_posts_for_translation.append({
            'text': text, 'timestamp': time_str, 'lang': lang_code,
            'handle': handle, 'post_url': post_url, 'did': did, 'profile_url': profile_url
        })

        if stats:
            stats.record_displayed(text_lower, keywords)

        if log_files:
            log_files.log_filter(text, time_str, matched, post_url, handle)

            for link in links:
                log_files.log_url(link)
                if stats:
                    stats.record_url()

            if profile_url:
                is_new = profile_url not in stats.unique_profiles if stats else True
                display_name = None
                if is_new and did:
                    info = fetch_bluesky_profile(did)
                    if info:
                        display_name = info.get('displayName', '')
                log_files.log_profile(profile_url, display_name)
                if stats:
                    stats.record_profile(profile_url, handle, display_name)

        if silent_mode:
            return

        # Display post
        print(f"\n{BRIGHT_CYAN}{'─' * 60}{RESET}")
        print(f"{BRIGHT_YELLOW}[{display_time}]{RESET} {BRIGHT_GREEN}{', '.join(matched)}{RESET}")
        lang_name = LANGUAGE_NAMES.get(lang_code, lang_code) if lang_code != 'unknown' else 'Unknown'
        print(f"{BRIGHT_MAGENTA}[{lang_name}]{RESET}")
        print(f"\n{BRIGHT_WHITE}{text}{RESET}")
        for link in links:
            print(f"{BRIGHT_RED}{link}{RESET}")

    except Exception:
        pass


def display_live_stats():
    """Display live statistics on screen."""
    global stats

    if not stats:
        return

    print("\033[2J\033[H", end="")  # Clear screen

    duration = stats.get_duration()
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed = duration.total_seconds()

    print(f"{BRIGHT_CYAN}╔══════════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BRIGHT_CYAN}║                  LIVE STATISTICS  (Press Q to quit)                  ║{RESET}")
    print(f"{BRIGHT_CYAN}╚══════════════════════════════════════════════════════════════════════╝{RESET}\n")

    print(f"{BRIGHT_WHITE}Running time:{RESET} {BRIGHT_YELLOW}{hours:02d}:{minutes:02d}:{seconds:02d}{RESET}\n")

    print(f"{BRIGHT_WHITE}STREAM:{RESET}")
    print(f"  Total posts:       {BRIGHT_GREEN}{stats.total_posts:,}{RESET}")
    print(f"  Posts/second:      {BRIGHT_GREEN}{stats.total_posts / elapsed if elapsed > 0 else 0:.1f}{RESET}")
    print(f"  Hashtags (total):  {BRIGHT_GREEN}{sum(stats.all_hashtag_counts.values()):,}{RESET}")
    print(f"  Unique hashtags:   {BRIGHT_GREEN}{len(stats.all_hashtag_counts):,}{RESET}")

    print(f"\n{BRIGHT_WHITE}FILTERED:{RESET}")
    print(f"  Matched posts:     {BRIGHT_GREEN}{stats.displayed_posts:,}{RESET}")
    if stats.total_posts > 0:
        print(f"  Match rate:        {BRIGHT_GREEN}{(stats.displayed_posts / stats.total_posts) * 100:.4f}%{RESET}")
    print(f"  Unique profiles:   {BRIGHT_GREEN}{len(stats.unique_profiles):,}{RESET}")
    print(f"  External URLs:     {BRIGHT_GREEN}{stats.total_urls:,}{RESET}")

    # Top 5 keywords
    print(f"\n{BRIGHT_WHITE}Top 5 Keyword matches:{RESET}")
    if stats.keyword_counts:
        sorted_kw = sorted([k for k in stats.keywords if stats.keyword_counts.get(k, 0) > 0],
                         key=lambda x: stats.keyword_counts.get(x, 0), reverse=True)
        for kw in sorted_kw[:5]:
            print(f"  {BRIGHT_YELLOW}{kw}{RESET}: {BRIGHT_GREEN}{stats.keyword_counts[kw]:,}{RESET}")
        if len(sorted_kw) > 5:
            print(f"  {BRIGHT_YELLOW}... and {len(sorted_kw) - 5} more keywords{RESET}")
    else:
        print(f"  {BRIGHT_YELLOW}No matches yet{RESET}")

    # Top 5 authors
    print(f"\n{BRIGHT_WHITE}Top 5 Authors (filtered):{RESET}")
    if stats.profile_post_counts:
        sorted_p = sorted(stats.profile_post_counts.items(), key=lambda x: x[1], reverse=True)
        for url, count in sorted_p[:5]:
            name = stats.profile_handles.get(url, '')
            print(f"  {BRIGHT_CYAN}{name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
            print(f"    {BRIGHT_WHITE}{url}{RESET}")
        if len(sorted_p) > 5:
            print(f"  {BRIGHT_YELLOW}... and {len(sorted_p) - 5} more authors{RESET}")
    else:
        print(f"  {BRIGHT_YELLOW}No authors yet{RESET}")

    # Top 5 languages
    print(f"\n{BRIGHT_WHITE}Top 5 Languages (filtered):{RESET}")
    if stats.language_counts:
        sorted_l = sorted(stats.language_counts.items(), key=lambda x: x[1], reverse=True)
        for code, count in sorted_l[:5]:
            pct = (count / stats.displayed_posts * 100) if stats.displayed_posts > 0 else 0
            print(f"  {BRIGHT_MAGENTA}{LANGUAGE_NAMES.get(code, code)}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} ({pct:.1f}%)")
    else:
        print(f"  {BRIGHT_YELLOW}No data yet{RESET}")

    # Top 5 hashtags
    print(f"\n{BRIGHT_WHITE}Top 5 Hashtags (filtered):{RESET}")
    if stats.hashtag_counts:
        sorted_h = sorted(stats.hashtag_counts.items(), key=lambda x: x[1], reverse=True)
        for tag, count in sorted_h[:5]:
            print(f"  {BRIGHT_CYAN}#{tag}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
        if len(sorted_h) > 5:
            print(f"  {BRIGHT_YELLOW}... and {len(sorted_h) - 5} more hashtags{RESET}")
    else:
        print(f"  {BRIGHT_YELLOW}No hashtags yet{RESET}")


async def check_keyboard():
    """Check for keyboard input (Q to quit)."""
    global quit_flag

    while not quit_flag:
        ready, _, _ = select.select([sys.stdin], [], [], 0.05)
        if ready:
            try:
                if sys.stdin.read(1).lower() == 'q':
                    print(f"\n\n{BRIGHT_YELLOW}>>> Shutting down...{RESET}\n")
                    quit_flag = True
                    break
            except Exception:
                pass
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


async def monitor_jetstream(keywords, keywords_lower, silent_mode=False):
    """Connect to Jetstream and monitor for posts."""
    global quit_flag

    if not silent_mode:
        print(f"\n{BRIGHT_WHITE}Connecting to Bluesky Jetstream...{RESET}")
        print(f"{BRIGHT_YELLOW}Monitoring for keywords: {', '.join(keywords)}{RESET}")
        print(f"{BRIGHT_CYAN}Press Q to quit (translations done after quit){RESET}\n")

    while not quit_flag:
        try:
            async with websockets.connect(JETSTREAM_URL, ping_interval=30, ping_timeout=10) as ws:
                if not silent_mode:
                    print(f"{BRIGHT_WHITE}Connected! Waiting for posts...{RESET}")

                ping_task = asyncio.create_task(websocket_ping(ws, 30))

                try:
                    while not quit_flag and not check_run_limits():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                            data = json.loads(msg)

                            if data.get('kind') == 'commit':
                                commit = data.get('commit', {})
                                if commit.get('operation') == 'create' and commit.get('collection') == 'app.bsky.feed.post':
                                    display_post(data, keywords, keywords_lower, silent_mode)
                                    check_run_limits()

                        except asyncio.TimeoutError:
                            check_run_limits()
                        except json.JSONDecodeError:
                            pass
                        except Exception:
                            pass
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosed:
            if not quit_flag and not silent_mode:
                print(f"\n{BRIGHT_YELLOW}Connection closed. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)
        except Exception as e:
            if not quit_flag and not silent_mode:
                print(f"\n{BRIGHT_YELLOW}Connection error: {e}. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)


async def periodic_flush(interval=5):
    """Periodically flush all log files."""
    global quit_flag, log_files

    while not quit_flag:
        await asyncio.sleep(interval)
        if log_files:
            log_files.flush_all()


async def run_monitor(keywords, keywords_lower, silent_mode=False):
    """Run the monitor with keyboard detection."""
    global quit_flag

    tasks = [
        asyncio.create_task(monitor_jetstream(keywords, keywords_lower, silent_mode)),
        asyncio.create_task(check_keyboard()),
        asyncio.create_task(periodic_flush(5)),
    ]

    if silent_mode:
        tasks.append(asyncio.create_task(update_live_stats(1)))

    while not quit_flag:
        await asyncio.wait([t for t in tasks if not t.done()], timeout=0.5, return_when=asyncio.FIRST_COMPLETED)

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
            return [line.strip() for line in f if line.strip()]
    return []


def save_keywords_to_file(keywords):
    """Save keywords to keywords.txt."""
    with open('keywords.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(keywords) + '\n')


def main():
    """Main entry point."""
    global stats, quit_flag, log_files, run_time_limit, run_post_limit

    print(f"\n{BRIGHT_CYAN}╔════════════════════════════════════════╗{RESET}")
    print(f"{BRIGHT_CYAN}║  Yeti - Bluesky Jetstream Monitor      ║{RESET}")
    print(f"{BRIGHT_CYAN}╚════════════════════════════════════════╝{RESET}\n")

    # Load or input keywords
    keywords = []
    saved = load_keywords_from_file()

    if saved:
        print(f"{BRIGHT_CYAN}Saved keywords found:{RESET}")
        for i, kw in enumerate(saved, 1):
            print(f"  {BRIGHT_WHITE}{i}. {kw}{RESET}")

        if input(f"\n{BRIGHT_CYAN}Use saved keywords? [Y/n]: {RESET}").strip().lower() != 'n':
            keywords = saved
            print(f"\n{BRIGHT_GREEN}Using saved keywords.{RESET}")
        else:
            print(f"\n{BRIGHT_CYAN}Enter new keywords to monitor (empty line to finish):{RESET}")

    if not keywords:
        if not saved:
            print(f"{BRIGHT_CYAN}Enter keywords to monitor (empty line to finish):{RESET}")

        while True:
            kw = input(f"  Keyword {len(keywords) + 1}: ").strip()
            if not kw:
                break
            keywords.append(kw)

        if not keywords:
            print("No keywords provided. Exiting.")
            return

        save_keywords_to_file(keywords)
        print(f"{BRIGHT_GREEN}Keywords saved to keywords.txt{RESET}")

    keywords_lower = [kw.lower() for kw in keywords]
    print(f"\n{BRIGHT_WHITE}Keywords: {', '.join(keywords)}{RESET}")

    # Run mode
    print(f"\n{BRIGHT_CYAN}Run mode:{RESET}")
    print(f"  {BRIGHT_WHITE}1{RESET} - Run until interrupted (default)")
    print(f"  {BRIGHT_WHITE}2{RESET} - Run for specific duration")
    print(f"  {BRIGHT_WHITE}3{RESET} - Run until specific number of filtered posts")
    run_mode = input("\nSelect run mode [1]: ").strip()

    run_time_limit = None
    run_post_limit = None

    if run_mode == "2":
        print(f"\n{BRIGHT_CYAN}Enter duration:{RESET}")
        try:
            h = int(input("  Hours [0]: ").strip() or "0")
            m = int(input("  Minutes [0]: ").strip() or "0")
            s = int(input("  Seconds [0]: ").strip() or "0")
            run_time_limit = h * 3600 + m * 60 + s
            if run_time_limit <= 0:
                print(f"{BRIGHT_YELLOW}Invalid duration. Running until interrupted.{RESET}")
                run_time_limit = None
            else:
                print(f"{BRIGHT_GREEN}Collection cycle: {h:02d}:{m:02d}:{s:02d} (auto-archives and continues){RESET}")
        except ValueError:
            print(f"{BRIGHT_YELLOW}Invalid input. Running until interrupted.{RESET}")

    elif run_mode == "3":
        try:
            count = int(input(f"\n{BRIGHT_CYAN}Number of filtered posts to collect: {RESET}").strip())
            if count > 0:
                run_post_limit = count
                print(f"{BRIGHT_GREEN}Will collect {count:,} filtered posts{RESET}")
            else:
                print(f"{BRIGHT_YELLOW}Invalid number. Running until interrupted.{RESET}")
        except ValueError:
            print(f"{BRIGHT_YELLOW}Invalid input. Running until interrupted.{RESET}")

    # Display mode
    print(f"\n{BRIGHT_CYAN}Display mode:{RESET}")
    print(f"  {BRIGHT_WHITE}1{RESET} - Show filtered posts (original text with language)")
    print(f"  {BRIGHT_WHITE}2{RESET} - Background mode (live statistics only) (default)")
    print(f"\n{BRIGHT_YELLOW}Note: All posts translated to EN & FI after pressing Q{RESET}")
    silent_mode = input("\nSelect mode [2]: ").strip() in ("2", "")

    # Initialize
    stats = Statistics(keywords)
    log_files = LogFiles()
    quit_flag = False

    ts = log_files.timestamp
    print(f"\n{BRIGHT_CYAN}Logging to:{RESET}")
    for name, desc in [
        (f"ALL.{ts}.log", "All posts from stream"),
        (f"FILTER.{ts}.log", "Filtered posts (keyword matches)"),
        (f"ENG_{ts}.log", "English translations"),
        (f"FIN_{ts}.log", "Finnish translations"),
        (f"EXT-URL.{ts}.log", "External URLs"),
        (f"PROFILES.{ts}.log", "Unique user profiles"),
        (f"FILTER_hashtag.{ts}.log", "Hashtags (filtered)"),
        (f"ALL_hashtag.{ts}.log", "Hashtags (all stream)"),
        (f"REPORT_{ts}.log", "Final report"),
    ]:
        print(f"  {BRIGHT_WHITE}{name}{RESET} - {desc}")

    old_settings = termios.tcgetattr(sys.stdin)

    try:
        tty.setcbreak(sys.stdin.fileno())
        asyncio.run(run_monitor(keywords, keywords_lower, silent_mode))
    except KeyboardInterrupt:
        pass
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        batch_translate_posts()
        stats.finish()
        if log_files:
            stats.print_report(log_files.get_report_filename())
            log_files.close()
        else:
            stats.print_report()


if __name__ == "__main__":
    main()
