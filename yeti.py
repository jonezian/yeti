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
from datetime import datetime

# ANSI color codes (bright versions)
BRIGHT_LIGHT_BLUE = "\033[1;96m"  # Bright cyan for translations
RESET = "\033[0m"
BRIGHT_CYAN = "\033[1;36m"
BRIGHT_YELLOW = "\033[1;93m"
BRIGHT_WHITE = "\033[1;97m"
BRIGHT_RED = "\033[1;91m"  # Bright red for links

# Jetstream WebSocket endpoint
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


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
    try:
        commit = post_data.get('commit', {})
        record = commit.get('record', {})

        text = record.get('text', '')
        if not text:
            return

        # Check if any keyword matches (case-insensitive)
        text_lower = text.lower()
        if not any(kw.lower() in text_lower for kw in keywords):
            return

        # Get metadata
        created_at = record.get('createdAt', '')
        langs = record.get('langs', [])

        # Format timestamp - convert to local time
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            local_dt = dt.astimezone()  # Convert to local timezone
            time_str = local_dt.strftime('%H:%M:%S')
        except:
            time_str = datetime.now().strftime('%H:%M:%S')

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

        # Print separator and timestamp
        print(f"\n{BRIGHT_CYAN}{'─' * 60}{RESET}")
        print(f"{BRIGHT_YELLOW}[{time_str}]{RESET}")

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


async def monitor_jetstream(keywords, finnish_only=False):
    """Connect to Jetstream and monitor for posts containing keywords."""
    print(f"\n{BRIGHT_WHITE}Connecting to Bluesky Jetstream...{RESET}")
    print(f"{BRIGHT_YELLOW}Monitoring for keywords: {', '.join(keywords)}{RESET}")
    if finnish_only:
        print(f"{BRIGHT_CYAN}Mode: Finnish only{RESET}")
    print(f"{BRIGHT_CYAN}Press Ctrl+C to stop{RESET}\n")

    while True:
        try:
            async with websockets.connect(JETSTREAM_URL) as websocket:
                print(f"{BRIGHT_WHITE}Connected! Waiting for posts...{RESET}")

                async for message in websocket:
                    try:
                        data = json.loads(message)

                        # Only process commit messages for posts
                        if data.get('kind') == 'commit':
                            commit = data.get('commit', {})
                            if commit.get('operation') == 'create':
                                if commit.get('collection') == 'app.bsky.feed.post':
                                    display_post(data, keywords, finnish_only)

                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        continue

        except websockets.exceptions.ConnectionClosed:
            print(f"\n{BRIGHT_YELLOW}Connection closed. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"\n{BRIGHT_YELLOW}Connection error: {e}. Reconnecting in 5 seconds...{RESET}")
            await asyncio.sleep(5)


def main():
    """Main entry point."""
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

    try:
        asyncio.run(monitor_jetstream(keywords, finnish_only))
    except KeyboardInterrupt:
        print(f"\n\n{BRIGHT_YELLOW}Monitoring stopped.{RESET}")


if __name__ == "__main__":
    main()
