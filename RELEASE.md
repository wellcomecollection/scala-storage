RELEASE_TYPE: minor

This changes the interface of `PrefixTransfer` to use `Future`, so it can run with concurrency and should be a bit faster.

There's also some refactoring in `S3Transfer` that means it should be faster in the case where the destination object doesn't exist.