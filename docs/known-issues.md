# Known Issues

Production readiness items to address after the DST coroutine refactor.

## Metrics and Observability

The library provides no metrics or observability hooks. Production deployments have no visibility into elections started, elections won, heartbeats sent/missed, leader step-downs, or dropped RPCs. Adding a metrics interface (or `log/slog` structured events) would let operators monitor cluster health without parsing debug logs.

## Fencing Token / Monotonic Lease Epoch

Leadership is communicated via callbacks and polling (`IsLeader`, `GetLeader`), but there is no mechanism for consumers to distinguish between two leadership periods. A fencing token or monotonic epoch that increments on every leadership change would let downstream systems reject stale writes from a leader that has since been superseded.
