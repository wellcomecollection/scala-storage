RELEASE_TYPE: minor

This release adds a `RetryableError` trait which indicates when an error is transient, and could be retried.  For example, a DynamoDB update() might fail with a `ConditionalCheckFailed` exception if two processes try to write at the same time.  The operation can be retried and will likely succeed.

Also, `UpdateNoSourceError`, `UpdateReadError` and `UpdateWriteError` now include the underlying `StorageError` as well as their `Throwable`.