from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class ExponentialJitter:
    base_delay: float = 0.5
    max_delay: float = 30.0
    factor: float = 2.0
    jitter: float = 0.25
    max_retries: int = sys.maxsize

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater than or equal to 0"

        if attempt > self.max_retries:
            return None

        if attempt == 0:
            return 0

        base = min(self.base_delay * (self.factor**attempt), self.max_delay)
        spread = base * self.jitter
        return base + random.uniform(-spread, spread)
