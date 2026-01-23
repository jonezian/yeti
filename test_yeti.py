#!/usr/bin/env python3
"""
Tests for Yeti - Bluesky Jetstream Monitor

Run with: python -m pytest test_yeti.py -v
"""

import pytest
from unittest.mock import MagicMock, patch

# Import functions and classes to test
from yeti import (
    format_time,
    format_number,
    sanitize_text,
    validate_url,
    safe_path,
    safe_json_loads,
    extract_hashtags,
    extract_links,
    Statistics,
    MAX_TEXT_LENGTH,
    MAX_JSON_SIZE,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for format_time()
# ═══════════════════════════════════════════════════════════════════════════════

def test_format_time_seconds():
    """Test format_time with values under 60 seconds."""
    assert format_time(0) == "0s"
    assert format_time(30) == "30s"
    assert format_time(59) == "59s"


def test_format_time_minutes():
    """Test format_time with values in minutes range."""
    assert format_time(60) == "1m 0s"
    assert format_time(90) == "1m 30s"
    assert format_time(3599) == "59m 59s"


def test_format_time_hours():
    """Test format_time with values in hours range."""
    assert format_time(3600) == "1h 0m 0s"
    assert format_time(3661) == "1h 1m 1s"
    assert format_time(7325) == "2h 2m 5s"


def test_format_time_negative():
    """Test format_time with negative value."""
    assert format_time(-1) == "—"
    assert format_time(-100) == "—"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for format_number()
# ═══════════════════════════════════════════════════════════════════════════════

def test_format_number_small():
    """Test format_number with small numbers."""
    assert format_number(0) == "0"
    assert format_number(999) == "999"
    assert format_number(9999) == "9,999"


def test_format_number_thousands():
    """Test format_number with numbers in thousands."""
    assert format_number(10000) == "10.0K"
    assert format_number(50000) == "50.0K"
    assert format_number(999999) == "1000.0K"  # Just under 1M, formatted as K


def test_format_number_millions():
    """Test format_number with numbers in millions."""
    assert format_number(1000000) == "1.0M"
    assert format_number(5500000) == "5.5M"
    assert format_number(10000000) == "10.0M"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for sanitize_text()
# ═══════════════════════════════════════════════════════════════════════════════

def test_sanitize_text_normal():
    """Test sanitize_text with normal input."""
    assert sanitize_text("Hello World") == "Hello World"
    assert sanitize_text("  trimmed  ") == "trimmed"


def test_sanitize_text_empty():
    """Test sanitize_text with empty input."""
    assert sanitize_text("") == ""
    assert sanitize_text(None) == ""
    assert sanitize_text("   ") == ""


def test_sanitize_text_length():
    """Test sanitize_text truncation."""
    long_text = "a" * 10000
    result = sanitize_text(long_text)
    assert len(result) == MAX_TEXT_LENGTH

    # Test with custom max_length
    result = sanitize_text(long_text, max_length=100)
    assert len(result) == 100


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for validate_url()
# ═══════════════════════════════════════════════════════════════════════════════

def test_validate_url_valid():
    """Test validate_url with valid URLs."""
    assert validate_url("https://example.com") is True
    assert validate_url("http://example.com/path?query=1") is True
    assert validate_url("https://sub.domain.example.com:8080/path") is True


def test_validate_url_invalid():
    """Test validate_url with invalid URLs."""
    assert validate_url("") is False
    assert validate_url(None) is False
    assert validate_url("not-a-url") is False
    assert validate_url("ftp://example.com") is False  # Only http/https allowed
    assert validate_url("javascript:alert(1)") is False


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for safe_json_loads()
# ═══════════════════════════════════════════════════════════════════════════════

def test_safe_json_loads_valid():
    """Test safe_json_loads with valid JSON."""
    result = safe_json_loads('{"key": "value"}')
    assert result == {"key": "value"}

    result = safe_json_loads('{"number": 123, "list": [1, 2, 3]}')
    assert result["number"] == 123
    assert result["list"] == [1, 2, 3]


def test_safe_json_loads_invalid():
    """Test safe_json_loads with invalid JSON."""
    assert safe_json_loads("") is None
    assert safe_json_loads(None) is None
    assert safe_json_loads("not json") is None
    assert safe_json_loads("{invalid}") is None


def test_safe_json_loads_too_large():
    """Test safe_json_loads with oversized input."""
    large_json = '{"data": "' + "a" * (MAX_JSON_SIZE + 1000) + '"}'
    result = safe_json_loads(large_json)
    assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for extract_hashtags()
# ═══════════════════════════════════════════════════════════════════════════════

def test_extract_hashtags_basic():
    """Test extract_hashtags with hashtags present."""
    record = {
        "facets": [
            {
                "features": [
                    {"$type": "app.bsky.richtext.facet#tag", "tag": "python"},
                    {"$type": "app.bsky.richtext.facet#tag", "tag": "programming"}
                ]
            }
        ]
    }
    result = extract_hashtags(record)
    assert result == ["python", "programming"]


def test_extract_hashtags_empty():
    """Test extract_hashtags with no hashtags."""
    assert extract_hashtags({}) == []
    assert extract_hashtags({"facets": []}) == []
    assert extract_hashtags({"facets": [{"features": []}]}) == []


def test_extract_hashtags_duplicates():
    """Test extract_hashtags doesn't return duplicates."""
    record = {
        "facets": [
            {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "python"}]},
            {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "python"}]},
        ]
    }
    result = extract_hashtags(record)
    assert result == ["python"]


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for extract_links()
# ═══════════════════════════════════════════════════════════════════════════════

def test_extract_links_basic():
    """Test extract_links with links present."""
    record = {
        "facets": [
            {
                "features": [
                    {"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com"},
                ]
            }
        ]
    }
    result = extract_links(record)
    assert result == ["https://example.com"]


def test_extract_links_empty():
    """Test extract_links with no links."""
    assert extract_links({}) == []
    assert extract_links({"facets": []}) == []


def test_extract_links_invalid_url():
    """Test extract_links filters out invalid URLs."""
    record = {
        "facets": [
            {
                "features": [
                    {"$type": "app.bsky.richtext.facet#link", "uri": "not-a-valid-url"},
                    {"$type": "app.bsky.richtext.facet#link", "uri": "https://valid.com"},
                ]
            }
        ]
    }
    result = extract_links(record)
    assert result == ["https://valid.com"]


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for Statistics class
# ═══════════════════════════════════════════════════════════════════════════════

def test_statistics_record_post():
    """Test Statistics.record_post()."""
    stats = Statistics(["test"])
    assert stats.total_posts == 0

    stats.record_post()
    assert stats.total_posts == 1

    for _ in range(99):
        stats.record_post()
    assert stats.total_posts == 100


def test_statistics_match_rate():
    """Test Statistics.match_rate()."""
    stats = Statistics(["test"])

    # No posts yet
    assert stats.match_rate() == 0

    # Add posts
    for _ in range(100):
        stats.record_post()

    stats.displayed_posts = 5
    assert stats.match_rate() == 5.0


def test_statistics_record_language():
    """Test Statistics.record_language()."""
    stats = Statistics(["test"])

    stats.record_language(["en"])
    stats.record_language(["en"])
    stats.record_language(["fi"])
    stats.record_language([])  # unknown

    assert stats.language_counts["en"] == 2
    assert stats.language_counts["fi"] == 1
    assert stats.language_counts["unknown"] == 1


def test_statistics_get_top_keywords():
    """Test Statistics.get_top_keywords()."""
    stats = Statistics(["alpha", "beta", "gamma", "delta"])

    stats.keyword_counts["alpha"] = 10
    stats.keyword_counts["beta"] = 5
    stats.keyword_counts["gamma"] = 15
    stats.keyword_counts["delta"] = 3

    top = stats.get_top_keywords(3)
    assert len(top) == 3
    assert top[0] == ("gamma", 15)
    assert top[1] == ("alpha", 10)
    assert top[2] == ("beta", 5)


def test_statistics_get_top_hashtags():
    """Test Statistics.get_top_hashtags()."""
    stats = Statistics(["test"])

    stats.hashtag_counts["python"] = 50
    stats.hashtag_counts["coding"] = 30
    stats.hashtag_counts["tech"] = 20

    top = stats.get_top_hashtags(2)
    assert len(top) == 2
    assert top[0] == ("python", 50)
    assert top[1] == ("coding", 30)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests for safe_path()
# ═══════════════════════════════════════════════════════════════════════════════

def test_safe_path_valid(tmp_path):
    """Test safe_path with valid paths."""
    # Create a temporary file
    test_file = tmp_path / "test.txt"
    test_file.write_text("test")

    result = safe_path(str(test_file))
    assert result is not None
    assert result.endswith("test.txt")


def test_safe_path_invalid():
    """Test safe_path with invalid paths."""
    assert safe_path("") is None
    assert safe_path(None) is None
    assert safe_path("/nonexistent/path/to/file.txt") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
