RELEASE_TYPE: minor

This change causes `VersionedDao.updateRecord` to throw a new exception
`DynamoNonFatalError` when encountering the following errors in DynamoDB:

-   Exceeding provisioned throughput limits
-   Hitting a conditional update exception

Both of these are errors where the caller can back off and retry -- as opposed
to an error that cannot be retried (for example, an authentication error).