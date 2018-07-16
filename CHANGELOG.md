# CHANGELOG

## v4.0.0 - 2018-07-16

Testin release automation again.

## v2.0.0 - 2018-07-05

Trigger for release automation

## v1.3.0 - 2018-07-04

This change causes `VersionedDao.updateRecord` to throw a new exception
`DynamoNonFatalError` when encountering the following errors in DynamoDB:

-   Exceeding provisioned throughput limits
-   Hitting a conditional update exception

Both of these are errors where the caller can back off and retry -- as opposed
to an error that cannot be retried (for example, an authentication error).

## v1.2.0 - 2018-06-14

This release deprecates the following method in `LocalVersionedHybridStore`:

```scala
def getJsonFor[T](bucket: Bucket, table: Table, record: T, id: String): String
```

in favour of:

```scala
def getJsonFor(bucket: Bucket, table: Table, id: String): String
```

Both methods behave in the same way -- the `record` parameter in the original
method had no effect.  Callers can remove the `record` parameter to upgrade.

(A bug in our automated release mechanism means there is no v1.1.0!)

## v1.0.3 - 2018-06-12

This release upgrades the AWS SDK used by scala-storage from 1.11.95 to
1.11.225.

## v1.0.2 - 2018-06-11

This is an attempt to expose the test packages (and in particular, the test
fixtures) in the published releases.

## v1.0.1 - 2018-06-11

This release moves a number of internal logs from INFO to DEBUG.

## v1.0.0 - 2018-06-11

Initial release!
