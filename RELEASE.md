RELEASE_TYPE: minor

Adds a new class `VHSEntry[M](hybridRecord: HybridRecord, metadata: M)` which is
returned as the result of a call to `updateRecord`.  Previous tuple unpacking should
still work.

Additionally, fixes a bug where `updateRecord` would return incorrect metadata when
updating an existing record.