# Fault-Tolerant Multi-Server Support for the Resonate Python SDK

> Bachelor's thesis project


---

## Abstract

The Resonate Python SDK provides durable, distributed-async-await execution by
checkpointing every function step into durable promises stored on a Resonate
Server. In its upstream form, the SDK talks to **one** server — if that server
becomes unreachable, every connected worker stalls until it comes back, even
when other healthy replicas exist.

This thesis extends the SDK with **transparent multi-server support**: a worker
can be configured with a list of server URLs and continue operating across
server crashes, network partitions, and rolling deployments without losing
work or requiring a restart. Health is tracked per-server with a circuit
breaker, and the same health view is shared between the HTTP store and the
SSE message poller so a failure observed by either side is immediately visible
to the other.

## Contributions

1. **`ServerPool` abstraction** — a `Protocol` decoupling the rest of the SDK
   from any specific routing strategy.
2. **Two pool implementations**:
   - `PriorityPool` — primary + warm standbys (failover only).
   - `RoundRobinPool` — even load distribution across healthy peers.
3. **Per-server `CircuitBreaker`** — `CLOSED → OPEN → HALF_OPEN → CLOSED`,
   tunable via constructor or environment variables.
4. **Shared pool wiring** — one pool instance is reused by `RemoteStore` and
   `Poller`, so health signals from real traffic and background `/ping`
   probes converge.
5. **Per-URL SSE poller** — the message source keeps one persistent SSE
   connection per server, matching how the broker fans messages out.
6. **Idempotent task delivery** — the bridge swallows the benign error codes
   that arise when several servers deliver the same task to a worker.
7. **Retry-policy hardening** — `ExponentialJitter` policy, and corrected
   `5xx` classification (502/503/504 are now retried, not raised).


## Repository layout

```
resonate/
├── circuit_breaker.py            # Per-server CB state machine        [new]
├── server_pool.py                # PriorityPool, RoundRobinPool       [new]
├── models/server_pool.py         # ServerPool Protocol                [new]
├── retry_policies/
│   └── exponential_jitter.py     # Jittered backoff                   [new]
├── stores/remote.py              # Pool-aware HTTP store              [modified]
├── message_sources/poller.py     # One persistent SSE thread per URL  [modified]
├── bridge.py                     # Duplicate-delivery tolerance       [modified]
└── resonate.py                   # `urls=` / `failover=` API          [modified]

tests/                            # Behavioral and fault-injection tests
MULTI_SERVER.md                   # Feature reference (user-facing)
```

## Quickstart

```shell
# Resonate Server & CLI
brew install resonatehq/tap/resonate

# SDK (development install)
uv sync
```

A worker with multi-server support:

```python
from resonate import Resonate, Context
from threading import Event

# Priority failover across two servers
resonate = Resonate(
    urls=["http://primary:8001", "http://secondary:8001"],
)

@resonate.register
def countdown(ctx: Context, count: int, delay: int):
    for i in range(count, 0, -1):
        yield ctx.run(ntfy, i)
        yield ctx.sleep(delay)


def ntfy(_: Context, i: int):
    print(f"Countdown: {i}")


resonate.start()
Event().wait()
```

Kill the primary server mid-countdown — the worker fails over to the secondary
within one circuit-breaker cycle and resumes from the last durable promise.


## Acknowledgements

- The [Resonate HQ](https://resonatehq.io) team for the upstream SDK and the
  distributed-async-await model that this work builds on.
