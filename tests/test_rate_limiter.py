from pysparkassist.api.rate_limiter import RateLimiter


def test_rate_limiter_allows_within_limit():
    limiter = RateLimiter(max_requests=5, window_seconds=60)
    for _ in range(5):
        assert limiter.is_allowed("127.0.0.1") is True


def test_rate_limiter_blocks_over_limit():
    limiter = RateLimiter(max_requests=2, window_seconds=60)
    assert limiter.is_allowed("127.0.0.1") is True
    assert limiter.is_allowed("127.0.0.1") is True
    assert limiter.is_allowed("127.0.0.1") is False


def test_rate_limiter_separate_ips():
    limiter = RateLimiter(max_requests=1, window_seconds=60)
    assert limiter.is_allowed("1.1.1.1") is True
    assert limiter.is_allowed("2.2.2.2") is True
    assert limiter.is_allowed("1.1.1.1") is False


def test_rate_limiter_window_slides_after_time_advances(monkeypatch):
    t = [0.0]

    def fake_time():
        return t[0]

    monkeypatch.setattr("pysparkassist.api.rate_limiter.time.time", fake_time)
    limiter = RateLimiter(max_requests=2, window_seconds=10)
    assert limiter.is_allowed("ip") is True
    assert limiter.is_allowed("ip") is True
    assert limiter.is_allowed("ip") is False
    t[0] = 11.0
    assert limiter.is_allowed("ip") is True
