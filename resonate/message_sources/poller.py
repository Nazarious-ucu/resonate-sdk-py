from __future__ import annotations

import logging
import queue
import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders import JsonEncoder
from resonate.models.message import InvokeMesg, Mesg, NotifyMesg, ResumeMesg
from resonate.server_pool import PriorityPool
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
    from resonate.models.server_pool import ServerPool

logger = logging.getLogger(__name__)


class Poller:
    def __init__(
        self,
        group: str,
        id: str,
        url: str | None = None,
        auth: tuple[str, str] | None = None,
        token: str | None = None,
        timeout: float | None = None,
        encoder: Encoder[Any, str] | None = None,
        pool: ServerPool | None = None,
    ) -> None:
        self._messages = queue.Queue[Mesg | None]()
        self._group = group
        self._id = id
        self._pool: ServerPool = pool or PriorityPool([url or "http://localhost:8001"])
        self._auth = auth
        self._token = token
        self._timeout = timeout
        self._encoder = encoder or JsonEncoder()
        self._stopped = False
        self._threads = [
            Thread(
                name=f"message-source::poller::{u}",
                target=self._loop_for_url,
                args=(u,),
                daemon=True,
            )
            for u in self._pool.urls
        ]

    @property
    def url(self) -> str:
        return f"{self._pool.active()}/poll/{self._group}/{self._id}"

    @property
    def unicast(self) -> str:
        return f"poll://uni@{self._group}/{self._id}"

    @property
    def anycast(self) -> str:
        return f"poll://any@{self._group}/{self._id}"

    def start(self) -> None:
        for t in self._threads:
            if not t.is_alive():
                t.start()

    def stop(self) -> None:
        # signal to consumer to disconnect
        self._messages.put(None)

        # TODO(avillega): Couldn't come up with a nice way of stoping this thread
        # iter_lines is blocking and request.get is also blocking, this makes it so
        # the only way to stop it is waiting for a timeout on the request itself
        # which could never happen.

        # This shutdown is only respected when the poller is instantiated with a timeout
        # value, which is not the default. This is still useful for tests.
        self._stopped = True

    def enqueue(self, mesg: Mesg) -> None:
        self._messages.put(mesg)

    def next(self) -> Mesg | None:
        return self._messages.get()

    @exit_on_exception
    def _loop_for_url(self, url: str) -> None:
        """Maintain one persistent SSE connection to a single fixed server URL."""
        delay = 2
        poll_url = f"{url}/poll/{self._group}/{self._id}"
        while not self._stopped:
            try:
                headers: dict[str, str] = {}
                auth = None
                if self._token:
                    headers["Authorization"] = f"Bearer {self._token}"
                elif self._auth:
                    auth = self._auth

                with requests.get(poll_url, auth=auth, headers=headers, stream=True, timeout=self._timeout) as res:
                    res.raise_for_status()
                    self._pool.report_success(url)

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        assert isinstance(line, str), "line must be a string"
                        if msg := self._process_line(line):
                            self._messages.put(msg)

            except requests.exceptions.RequestException:
                self._pool.report_failure(url)
                logger.warning("Networking. Cannot connect to %s. Retrying in %s sec.", url, delay)
                time.sleep(delay)
            except Exception:
                self._pool.report_failure(url)
                logger.warning("Networking. Cannot connect to %s. Retrying in %s sec.", url, delay)
                time.sleep(delay)

    def _process_line(self, line: str) -> Mesg | None:
        if not line:
            return None

        stripped = line.strip()
        if not stripped.startswith("data:"):
            return None

        d = self._encoder.decode(stripped[5:])
        match d["type"]:
            case "invoke":
                return InvokeMesg(type="invoke", task=d["task"])
            case "resume":
                return ResumeMesg(type="resume", task=d["task"])
            case "notify":
                return NotifyMesg(type="notify", promise=d["promise"])
            case _:
                # Unknown message type
                return None
