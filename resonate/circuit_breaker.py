from __future__ import annotations

import threading
import time
from typing import Literal


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
    ) -> None:
        if not isinstance(failure_threshold, int) or failure_threshold < 1:
            msg = f"failure_threshold must be a positive `int`, got {failure_threshold!r}"
            raise ValueError(msg)
        if not isinstance(recovery_timeout, (int, float)) or recovery_timeout <= 0:
            msg = f"recovery_timeout must be a positive number, got {recovery_timeout!r}"
            raise ValueError(msg)
        if not isinstance(half_open_max_calls, int) or half_open_max_calls < 1:
            msg = f"half_open_max_calls must be a positive `int`, got {half_open_max_calls!r}"
            raise ValueError(msg)

        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._state: Literal["CLOSED", "OPEN", "HALF_OPEN"] = "CLOSED"
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> Literal["CLOSED", "OPEN", "HALF_OPEN"]:
        with self._lock:
            return self._state

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == "CLOSED":
                return True
            if self._state == "OPEN":
                if (
                    self._last_failure_time is not None
                    and time.monotonic() - self._last_failure_time >= self._recovery_timeout
                ):
                    self._state = "HALF_OPEN"
                    self._half_open_calls = 0
                return self._state == "HALF_OPEN"
            # HALF_OPEN
            if self._half_open_calls < self._half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = "CLOSED"
            self._half_open_calls = 0

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == "HALF_OPEN" or (
                self._state == "CLOSED" and self._failure_count >= self._failure_threshold
            ):
                self._state = "OPEN"
