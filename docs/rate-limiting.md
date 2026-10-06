# Rate limiting

The package implements three atomic algorithms:

| Algorithm | Storage | Best for |
| --- | --- | --- |
| Fixed window | One counter per time boundary | Lowest cost and simple quotas |
| Sliding window | Current and previous counters | Fewer boundary bursts |
| Token bucket | Tokens and last-refill timestamp | Controlled bursts and sustained rate |

All algorithms execute their state transition in one Redis script. Identifiers are
SHA-256 hashed and namespaced with the configured prefix. Weighted requests subtract
more than one unit from the current quota.

Optional local blocked-result caching reduces Redis calls after an identifier has been
denied. Keep the limiter outside hot serverless handlers when using that cache.

`failureMode: "closed"` propagates Redis failures. `failureMode: "open"` returns a
successful timeout/Redis result and should only be used when availability is the
stronger requirement.

Analytics are delivered to an injected `RateLimitAnalyticsSink`; the package does not
force a logging or metrics vendor.
