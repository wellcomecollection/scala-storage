RELEASE_TYPE: minor

When calling `VersionedDao.getRecord`, a `DynamoThroughputExceededException`
is wrapped as a `DynamoNonFatalError` in the same was as `updateRecord`.
