RELEASE_TYPE: minor

A bunch of fixes that came from integrating the new library into the storage-service repo:

*   Add an S3-agnostic generator for random instances of `ObjectLocation`
*   Add an underlying trait `PrefixCopier` that has two implementations: `S3PrefixCopier` and `MemoryPrefixCopier`.
    The latter is suitable for use in tests.
