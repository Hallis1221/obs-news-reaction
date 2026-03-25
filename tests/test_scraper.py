"""Tests for news scraper normalization logic."""

from obs_news_reaction.news.scraper import _normalize, _extract_messages


def test_normalize_standard_fields():
    raw = {
        "messageId": "12345",
        "ticker": "eqnr",
        "publishedTime": "2025-01-15T10:30:00Z",
        "category": "INSIDER",
        "title": "CEO buys shares",
        "url": "https://newsweb.oslobors.no/message/12345",
    }
    result = _normalize(raw)
    assert result is not None
    assert result["message_id"] == "12345"
    assert result["ticker"] == "EQNR"
    assert result["category"] == "INSIDER"
    assert "2025-01-15" in result["published_at"]


def test_normalize_alternative_field_names():
    raw = {
        "disclosureId": 99,
        "issuerSign": "dnb",
        "publishedAt": "2025-03-01T08:00:00+01:00",
        "categoryName": "FINANCIAL",
        "headline": "Q4 Results",
    }
    result = _normalize(raw)
    assert result is not None
    assert result["message_id"] == "99"
    assert result["ticker"] == "DNB"
    assert result["title"] == "Q4 Results"


def test_normalize_no_ticker():
    raw = {"messageId": "1", "title": "Test"}
    result = _normalize(raw)
    assert result is None


def test_normalize_no_message_id():
    raw = {"ticker": "EQNR"}
    result = _normalize(raw)
    assert result is None


def test_normalize_issuer_nested():
    raw = {
        "id": "500",
        "issuer": {"sign": "NHY"},
        "published": "2025-02-01T12:00:00Z",
        "type": "OTHER",
        "subject": "Annual report",
    }
    result = _normalize(raw)
    assert result is not None
    assert result["ticker"] == "NHY"
    assert result["title"] == "Annual report"


def test_extract_messages_from_list():
    data = [
        {"messageId": "1", "ticker": "A", "title": "T1"},
        {"messageId": "2", "ticker": "B", "title": "T2"},
    ]
    msgs = _extract_messages(data)
    assert len(msgs) == 2


def test_extract_messages_from_dict_messages_key():
    data = {
        "messages": [
            {"messageId": "1", "ticker": "A", "title": "T1"},
        ]
    }
    msgs = _extract_messages(data)
    assert len(msgs) == 1


def test_extract_messages_empty():
    assert _extract_messages([]) == []
    assert _extract_messages({}) == []
