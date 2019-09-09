RELEASE_TYPE: minor

This release adds a `RetryOps` class for retrying functions which have non-deterministic results.  In particular, any function that returns a `RetryableError` will be retried.

Usage:

```scala
val f: In => Either[OutError, Out]

val retryableFunction = (f _).retry(maxAttempts = 3)
retryableFunction("x", "y", "z")
```

This retrying behaviour is used in `S3StreamStore.get()`, which by default now retries any transient error from S3 before failing.