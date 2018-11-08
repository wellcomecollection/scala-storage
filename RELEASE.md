RELEASE_TYPE: minor

Adds a new class `VHSIndexEntry[M](hybridRecord: HybridRecord, metadata: M)` which is
returned as the result of a call to `updateRecord`.

Additionally, fixes a bug where `updateRecord` would return incorrect metadata when
updating an existing record.
