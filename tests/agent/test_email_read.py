import logging

from agent.email_read import extract_codes, fetch_codes


def test_extract_codes_finds_digits_and_links():
    text = "Your Anthropic code is 482913. Or click https://claude.ai/verify?t=abc"
    found = extract_codes(text)
    assert "482913" in found and any(f.startswith("https://") for f in found)


def test_extract_ignores_short_and_long_digit_runs():
    assert extract_codes("call 123 or 123456789012") == []


def test_fetch_codes_never_logs_values(caplog):
    # Review Focus #1
    fake_messages = [{"received_at": "2026-09-22T07:00:00Z",
                      "from": "no-reply@anthropic.com",
                      "subject": "Your sign-in code",
                      "body": "code 482913"}]
    with caplog.at_level(logging.DEBUG):
        out = fetch_codes("claude", transport=lambda since: fake_messages,
                          providers={"claude": {"code_patterns": {
                              "senders": ["anthropic.com"],
                              "subject_regexes": ["sign.?in"]}}})
    assert out["ok"] and out["codes"][0]["code_or_link"] == "482913"
    assert "482913" not in caplog.text


def test_non_matching_sender_excluded():
    fake = [{"received_at": "2026-09-22T07:00:00Z", "from": "evil@phish.com",
             "subject": "sign-in code", "body": "code 999999"}]
    out = fetch_codes("claude", transport=lambda since: fake,
                      providers={"claude": {"code_patterns": {
                          "senders": ["anthropic.com"], "subject_regexes": ["sign.?in"]}}})
    assert out["codes"] == []


def test_transport_failure_is_ok_false():
    def boom(since):
        raise RuntimeError("graph down")
    out = fetch_codes("claude", transport=boom,
                      providers={"claude": {"code_patterns": {"senders": ["x"],
                                                              "subject_regexes": ["y"]}}})
    assert out == {"ok": False, "codes": [], "error": out["error"]}
