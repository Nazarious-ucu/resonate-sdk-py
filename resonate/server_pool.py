from __future__ import annotations

import logging
import threading
import time
from threading import Thread

import requests

from resonate.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class PriorityPool:
    """Routes every request to the highest-priority healthy server.

    Servers are tried in the order they appear in ``urls``. The first one
    whose circuit breaker allows a request is used. Falls back to the first
    URL when all breakers are open.

    Health checks interval is 10s by default.
    """

    def __init__(
        self,
        urls: list[str],
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        health_check_interval: float | None = 10.0,
        health_check_timeout: float = 2.0,
        health_check_path: str = "/ping",
    ) -> None:
        if not urls:
            msg = "urls must be a non-empty list"
            raise ValueError(msg)

        self._urls = list(urls)
        self._breakers: dict[str, CircuitBreaker] = {
            url: CircuitBreaker(
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
            for url in self._urls
        }
        self._health_check_interval = health_check_interval
        self._health_check_timeout = health_check_timeout
        self._health_check_path = health_check_path
        self._stopped = False
        self._thread: Thread | None = (
            Thread(
                name="server-pool::health-check",
                target=self._health_check_loop,
                daemon=True,
            )
            if health_check_interval is not None
            else None
        )

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def active(self) -> str:
        for url in self._urls:
            if self._breakers[url].allow_request():
                return url
        # All breakers open — return primary as fallback so retries eventually probe it
        return self._urls[0]

    def report_success(self, url: str) -> None:
        if url in self._breakers:
            self._breakers[url].record_success()

    def report_failure(self, url: str) -> None:
        if url in self._breakers:
            self._breakers[url].record_failure()

    def start(self) -> None:
        if self._thread is not None and not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stopped = True

    def _health_check_loop(self) -> None:
        assert self._health_check_interval is not None
        while not self._stopped:
            for url in self._urls:
                try:
                    res = requests.get(f"{url}{self._health_check_path}", timeout=self._health_check_timeout)
                    if res.ok:
                        self._breakers[url].record_success()
                        logger.debug("Health check OK: %s", url)
                    else:
                        self._breakers[url].record_failure()
                        logger.warning("Health check failed for %s (status %s)", url, res.status_code)
                except Exception:
                    self._breakers[url].record_failure()
                    logger.warning("Health check unreachable: %s", url)
            time.sleep(self._health_check_interval)


class RoundRobinPool:
    """Distributes load evenly across all healthy servers.

    Health checks interval is 10s by default.
    """

    def __init__(
        self,
        urls: list[str],
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        health_check_interval: float | None = 10.0,
        health_check_timeout: float = 2.0,
        health_check_path: str = "/ping",
    ) -> None:
        if not urls:
            msg = "urls must be a non-empty list"
            raise ValueError(msg)

        self._urls = list(urls)
        self._breakers: dict[str, CircuitBreaker] = {
            url: CircuitBreaker(
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
            for url in self._urls
        }
        self._index = 0
        self._lock = threading.Lock()
        self._health_check_interval = health_check_interval
        self._health_check_timeout = health_check_timeout
        self._health_check_path = health_check_path
        self._stopped = False
        self._thread: Thread | None = (
            Thread(
                name="server-pool::health-check",
                target=self._health_check_loop,
                daemon=True,
            )
            if health_check_interval is not None
            else None
        )

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def active(self) -> str:
        with self._lock:
            for i in range(len(self._urls)):
                url = self._urls[(self._index + i) % len(self._urls)]
                if self._breakers[url].allow_request():
                    self._index = (self._index + i + 1) % len(self._urls)
                    return url
            # All breakers open — return current as fallback
            return self._urls[self._index % len(self._urls)]

    def report_success(self, url: str) -> None:
        if url in self._breakers:
            self._breakers[url].record_success()

    def report_failure(self, url: str) -> None:
        if url in self._breakers:
            self._breakers[url].record_failure()

    def start(self) -> None:
        if self._thread is not None and not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stopped = True

    def _health_check_loop(self) -> None:
        assert self._health_check_interval is not None
        while not self._stopped:
            for url in self._urls:
                try:
                    res = requests.get(f"{url}{self._health_check_path}", timeout=self._health_check_timeout)
                    if res.ok:
                        self._breakers[url].record_success()
                        logger.debug("Health check OK: %s", url)
                    else:
                        self._breakers[url].record_failure()
                        logger.warning("Health check failed for %s (status %s)", url, res.status_code)
                except Exception:
                    self._breakers[url].record_failure()
                    logger.warning("Health check unreachable: %s", url)
            time.sleep(self._health_check_interval)
