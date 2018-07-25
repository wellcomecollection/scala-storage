# CHANGELOG

## v1.5.1 - 2018-07-25

This is a change to the repo build scripts that enable auto-formatting with
scalafmt, but there shouldn't be any change to the external APIs.

## v1.5.0 - 2018-07-24

This release changes the hashing algorithm used in `SerialisationStrategy`.

Previously we used MurmurHash3, which turned out to be more vulnerable to
collisions than we expected -- now we use SHA-256 instead.

## v1.4.0 - 2018-07-23

This removes the `GlobalExecutionContext` from the library, an internal helper
that was never actually used, and a holdover from when this library was
part of the main platform repo.

## v1.3.0 - 2018-07-20

This patch adds a new helper to `S3` for saving a complete record of
everything in the bucket:

```scala
def getAllObjectContents(bucket: Bucket): Map[String, String]
```

## v1.2.0 - 2018-07-18

This release has no changes, it just switches our automated release process to
[fm-sbt-s3-resolver](https://github.com/frugalmechanic/fm-sbt-s3-resolver).

## v1.1.0 - 2018-07-17

This adds a helper to `LocalVersionedHybridStore` that was missing from the
migration from the main repo:

```scala
def assertStored[T](bucket: Bucket, table: Table, id: String, record: T)(
  implicit encoder: Encoder[T])
```

## v1.0.0 - 2018-07-17

This release has no code changes, but marks parity with the storage library
from the [main platform repo](https://github.com/wellcometrust/platform).

## v0.2.0 - 2018-07-17

This patch adds two helpers to the S3 fixture:

```scala
def getObjectFromS3[T](bucket: Bucket, key: String)(
  implicit decoder: Decoder[T]): T

def listKeysInBucket(bucket: Bucket): List[String]
```

## v0.1.0 - 2018-07-17

This release adds two new fixtures to `LocalDynamoDb`:

```scala
def withSpecifiedLocalDynamoDbTable[R](
  createTable: (AmazonDynamoDB) => Table): Fixture[Table, R]

def withVersionedDao[R](table: Table)(testWith: TestWith[VersionedDao, R]): R
```

and some new helper methods for use in tests:

```scala
def givenTableHasItem[T: DynamoFormat](item: T, table: Table): Assertion

def getTableItem[T: DynamoFormat](id: String, table: Table): Assertion

def getExistingTableItem[T: DynamoFormat](id: String, table: Table): Assertion

def assertTableEmpty[T: DynamoFormat](table: Table): Assertion

def assertTableHasItem[T: DynamoFormat](
  id: String, item: T, table: Table): Assertion

def assertTableOnlyHasItem[T: DynamoFormat](item: T, table: Table): Assertion
```

## v0.0.3 - 2018-07-17

This patch fixes a bug in the `S3` fixture where it could attempt to create
buckets before the Docker container was responding to requests, leading
to intermittent test failures.

## v0.0.2 - 2018-07-17

This patch fixes a bug in `S3StorageBackend`, where the following warning
would be emitted when uploading a file to S3:

> No content length specified for stream data.  Stream contents will be
> buffered in memory and could result in out of memory errors.

We're now using the S3 API correctly, so this error can't occur (and the
warning goes away).

## v0.0.2 - 2018-07-17

First release cut with the automated releases to S3!

This release fixes a race condition in the `LocalDynamoDb` fixture.  When
cleaning up a table, the fixture would clean up _every_ table -- even tables
created by a different test.  This caused intermittent test failures when
running tests in parallel.

## v0.0.1 - 2018-07-16

Initial release with the new S3 storage!
