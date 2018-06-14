# CHANGELOG

## v1.0.4 - 2018-06-14

This release fixes some reliability issues in tests caused by the s3 mock container not starting fast enough.

## v1.1.0 - 2018-06-14

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
