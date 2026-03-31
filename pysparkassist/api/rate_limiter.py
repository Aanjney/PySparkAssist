import time
from collections import defaultdict


class RateLimiter:
    def __init__(self, max_requests: int = 20, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now = time.time()
        cutoff = now - self.window_seconds

        timestamps = [t for t in self._requests[ip] if t > cutoff]

        if not timestamps:
            self._requests.pop(ip, None)

        if len(timestamps) >= self.max_requests:
            self._requests[ip] = timestamps
            return False

        timestamps.append(now)
        self._requests[ip] = timestamps
        return True
