from pysparkassist.api.groq_limits_store import load_groq_limits, save_groq_limits


def test_save_and_load_roundtrip(tmp_path):
    path = str(tmp_path / "limits.json")
    data = {
        "remaining_requests": 999,
        "limit_requests": 1000,
        "remaining_tokens": 25000,
        "limit_tokens": 30000,
        "fetched_at": 1700000000.0,
    }
    save_groq_limits(path, data)
    loaded = load_groq_limits(path)
    assert loaded == data


def test_load_missing_returns_none(tmp_path):
    assert load_groq_limits(str(tmp_path / "nope.json")) is None
