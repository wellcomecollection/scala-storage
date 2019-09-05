RELEASE_TYPE: patch

This fixes a pair of race conditions in VersionedStore where multiple processes calling `putLatest(…)` simultaneously could return a `VersionAlreadyExistsError` or `HigherVersionExistsError`.  If there is an error, it should return `StoreWriteError`, with `RetryableError` as appropriate.