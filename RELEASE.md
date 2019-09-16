RELEASE_TYPE: patch

Change the behaviour of two methods in VersionedStore (update and upsert) to return a `RetryableError` in race conditions where two or more processes try to write simultaneously.

(See also: v7.24.0.)