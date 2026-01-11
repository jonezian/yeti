#!/usr/bin/env python3
"""
Bluesky Jetstream Monitor - Real-time post monitoring with keyword filtering
Connects to Bluesky Jetstream WebSocket service and displays posts matching keywords.
Non-Finnish posts are automatically translated to Finnish.
"""

import json
import asyncio
import websockets
import requests
import sys
import select
import tty
import termios
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

    def record_post(self, langs):
        """Record a post from the stream."""
        self.total_posts += 1
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
        """Print the statistics report."""
        duration = self.get_duration()
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        print(f"\n\n{BRIGHT_CYAN}{'═' * 60}{RESET}")
        print(f"{BRIGHT_CYAN}                    SESSION REPORT{RESET}")
        print(f"{BRIGHT_CYAN}{'═' * 60}{RESET}\n")

        # Time info
        print(f"{BRIGHT_WHITE}Time:{RESET}")
        print(f"  Started:  {BRIGHT_YELLOW}{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Ended:    {BRIGHT_YELLOW}{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
        print(f"  Duration: {BRIGHT_YELLOW}{hours:02d}:{minutes:02d}:{seconds:02d}{RESET}")

        # Post counts
        print(f"\n{BRIGHT_WHITE}Posts:{RESET}")
        print(f"  Total from stream: {BRIGHT_GREEN}{self.total_posts:,}{RESET}")
        print(f"  Displayed:         {BRIGHT_GREEN}{self.displayed_posts:,}{RESET}")
        if self.total_posts > 0:
            percentage = (self.displayed_posts / self.total_posts) * 100
            print(f"  Match rate:        {BRIGHT_GREEN}{percentage:.2f}%{RESET}")

        # Keyword stats
        print(f"\n{BRIGHT_WHITE}Keyword matches:{RESET}")
        if self.keyword_counts:
            for kw in self.keywords:
                count = self.keyword_counts.get(kw, 0)
                print(f"  {BRIGHT_YELLOW}{kw}{RESET}: {BRIGHT_GREEN}{count:,}{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No matches{RESET}")

        # Language stats
        print(f"\n{BRIGHT_WHITE}Languages:{RESET}")
        if self.language_counts:
            # Sort by count descending
            sorted_langs = sorted(self.language_counts.items(), key=lambda x: x[1], reverse=True)
            # Show top 15 languages
            for lang_code, count in sorted_langs[:15]:
                percentage = (count / self.total_posts) * 100 if self.total_posts > 0 else 0
                lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
                print(f"  {BRIGHT_MAGENTA}{lang_name}{RESET}: {BRIGHT_GREEN}{count:,}{RESET} ({percentage:.1f}%)")
            if len(sorted_langs) > 15:
                print(f"  {BRIGHT_YELLOW}... and {len(sorted_langs) - 15} more languages{RESET}")
        else:
            print(f"  {BRIGHT_YELLOW}No language data{RESET}")

        print(f"\n{BRIGHT_CYAN}{'═' * 60}{RESET}\n")


# Global statistics instance
stats = None
# Global flag for quit
quit_flag = False
# Global log files
log_files = None


class LogFiles:
    """Handle logging to multiple files."""

    def __init__(self):
        self.full_log = open('full.log', 'w', encoding='utf-8')
        self.posts_log = open('posts.log', 'w', encoding='utf-8')
        self.translated_log = open('suomennettu.log', 'w', encoding='utf-8')
        self.urls_log = open('URLs.log', 'w', encoding='utf-8')

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
        self.translated_log.write(f"Finnish: {translation}\n\n")
        self.translated_log.flush()

    def log_url(self, url):
        """Log external URLs only."""
        self.urls_log.write(f"{url}\n")
        self.urls_log.flush()

    def close(self):
        """Close all log files."""
        self.full_log.close()
        self.posts_log.close()
        self.translated_log.close()
        self.urls_log.close()


def translate_to_finnish(text):
    """Translate text to Finnish using Google Translate API."""
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
            return translated
    except Exception as e:
        return None
    return None


def is_finnish(text, langs=None):
    """
    Detect if text is Finnish based on language tag or linguistic patterns.
    """
    # Check language tags first
    if langs:
        for lang in langs:
            if lang.lower().startswith('fi'):
                return True

    # Fallback to pattern matching
    text_lower = text.lower()
    score = 0

    # Finnish special characters
    for char in ['ä', 'ö', 'å']:
        score += text_lower.count(char) * 2

    # Finnish double vowels
    double_vowels = ['aa', 'ee', 'ii', 'oo', 'uu', 'yy', 'ää', 'öö']
    for dv in double_vowels:
        if dv in text_lower:
            score += 1

    # Common Finnish words
    finnish_words = ['ja', 'on', 'ei', 'se', 'että', 'kun', 'niin', 'kuin',
                     'oli', 'olla', 'joka', 'mutta', 'tai', 'jos', 'vain',
                     'nyt', 'jo', 'vielä', 'sitten', 'koska', 'kanssa',
                     'minä', 'sinä', 'hän', 'me', 'te', 'he', 'tämä', 'tuo']

    words = text_lower.split()
    for word in words:
        if word in finnish_words:
            score += 1

    return score >= 3


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


def display_post(post_data, keywords, finnish_only=False):
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
        if not any(kw.lower() in text_lower for kw in keywords):
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

        # Check if post is Finnish
        post_is_finnish = is_finnish(text, langs)

        # Get translation if needed
        translation = None
        if not post_is_finnish:
            translation = translate_to_finnish(text)
            if translation and translation.lower() == text.lower():
                translation = None  # No real translation

        # Finnish only mode: skip posts that can't be translated
        if finnish_only and not post_is_finnish and not translation:
            return

        # Record displayed post
        if stats:
            stats.record_displayed(text, keywords)

        # Log filtered post to posts.log
        if log_files:
            log_files.log_post(text, time_str)

        # Log translation to suomennettu.log
        if log_files and translation:
            log_files.log_translated(text, translation, time_str)

        # Log external URLs to URLs.log
        if log_files:
            for link in links:
                log_files.log_url(link)

        # Print separator and timestamp
        print(f"\n{BRIGHT_CYAN}{'─' * 60}{RESET}")
        print(f"{BRIGHT_YELLOW}[{display_time}]{RESET}")

        if finnish_only:
            # Show only Finnish content in bright white
            if post_is_finnish:
                print(f"\n{BRIGHT_WHITE}{text}{RESET}")
            elif translation:
                print(f"\n{BRIGHT_WHITE}{translation}{RESET}")
        else:
            # Show both original and translation
            print(f"\n{BRIGHT_WHITE}{text}{RESET}")
            if translation:
                print(f"\n{BRIGHT_LIGHT_BLUE}[FI] {translation}{RESET}")

        # Print external links in bright red
        for link in links:
            print(f"{BRIGHT_RED}{link}{RESET}")

    except Exception as e:
        pass  # Silently skip malformed posts


async def check_keyboard():
    """Check for keyboard input (Q to quit)."""
    global quit_flag

    loop = asyncio.get_event_loop()

    while not quit_flag:
        # Check if there's input available
        ready, _, _ = select.select([sys.stdin], [], [], 0.1)
        if ready:
            char = sys.stdin.read(1)
            if char.lower() == 'q':
                quit_flag = True
                break
        await asyncio.sleep(0.1)


async def monitor_jetstream(keywords, finnish_only=False):
    """Connect to Jetstream and monitor for posts containing keywords."""
    global quit_flag

    print(f"\n{BRIGHT_WHITE}Connecting to Bluesky Jetstream...{RESET}")
    print(f"{BRIGHT_YELLOW}Monitoring for keywords: {', '.join(keywords)}{RESET}")
    if finnish_only:
        print(f"{BRIGHT_CYAN}Mode: Finnish only{RESET}")
    print(f"{BRIGHT_CYAN}Press Q to quit{RESET}\n")

    while not quit_flag:
        try:
            async with websockets.connect(JETSTREAM_URL) as websocket:
                print(f"{BRIGHT_WHITE}Connected! Waiting for posts...{RESET}")

                while not quit_flag:
                    try:
                        # Wait for message with timeout to check quit flag
                        message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                        data = json.loads(message)

                        # Only process commit messages for posts
                        if data.get('kind') == 'commit':
                            commit = data.get('commit', {})
                            if commit.get('operation') == 'create':
                                if commit.get('collection') == 'app.bsky.feed.post':
                                    display_post(data, keywords, finnish_only)

                    except asyncio.TimeoutError:
                        continue
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        continue

        except websockets.exceptions.ConnectionClosed:
            if not quit_flag:
                print(f"\n{BRIGHT_YELLOW}Connection closed. Reconnecting in 5 seconds...{RESET}")
                await asyncio.sleep(5)
        except Exception as e:
            if not quit_flag:
                print(f"\n{BRIGHT_YELLOW}Connection error: {e}. Reconnecting in 5 seconds...{RESET}")
                await asyncio.sleep(5)


async def run_monitor(keywords, finnish_only=False):
    """Run the monitor with keyboard detection."""
    global quit_flag

    # Create tasks for monitoring and keyboard checking
    monitor_task = asyncio.create_task(monitor_jetstream(keywords, finnish_only))
    keyboard_task = asyncio.create_task(check_keyboard())

    # Wait for either task to complete
    done, pending = await asyncio.wait(
        [monitor_task, keyboard_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    # Cancel pending tasks
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def main():
    """Main entry point."""
    global stats, quit_flag, log_files

    print(f"\n{BRIGHT_CYAN}╔════════════════════════════════════════╗{RESET}")
    print(f"{BRIGHT_CYAN}║  Bluesky Jetstream Keyword Monitor     ║{RESET}")
    print(f"{BRIGHT_CYAN}╚════════════════════════════════════════╝{RESET}\n")

    # Collect keywords
    keywords = []
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

    print(f"\n{BRIGHT_WHITE}Keywords: {', '.join(keywords)}{RESET}")

    # Ask for display mode
    print(f"\n{BRIGHT_CYAN}Display mode:{RESET}")
    print(f"  {BRIGHT_WHITE}1{RESET} - Show original + Finnish translation (default)")
    print(f"  {BRIGHT_WHITE}2{RESET} - Show only Finnish (translated){RESET}")
    mode_choice = input("\nSelect mode [1]: ").strip()

    finnish_only = mode_choice == "2"

    # Initialize statistics and log files
    stats = Statistics(keywords)
    log_files = LogFiles()
    quit_flag = False

    print(f"\n{BRIGHT_CYAN}Logging to: full.log, posts.log, suomennettu.log, URLs.log{RESET}")

    # Save terminal settings
    old_settings = termios.tcgetattr(sys.stdin)

    try:
        # Set terminal to raw mode for single character input
        tty.setcbreak(sys.stdin.fileno())

        asyncio.run(run_monitor(keywords, finnish_only))

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
