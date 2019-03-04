# CHANGELOG

## v3.4.0 - 2019-03-04

This release adds three new classes and a new trait: S3PrefixCopier, S3Copier, S3PrefixOperator and ObjectCopier.

It also makes the AmazonS3 client in the S3 fixture implicit.

## v3.3.0 - 2019-02-08

This release adds the `storage_typesafe` library for configuring the `storage` library using Typesafe.

## v3.2.1 - 2019-02-05

Start using the scala-fixtures lib rather than vendoring fixtures.

## v3.2.0 - 2019-01-30

The VHS no longer includes a shard in S3 keys.  This is based on
[updates to S3][no_shards_required] that mean sharding is no longer required:

> This S3 request rate performance increase removes any previous guidance to randomize object prefixes to achieve faster performance. That means you can now use logical or sequential naming patterns in S3 object naming without any performance implications.

[no_shards_required]: https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/

## v3.1.0 - 2018-12-07

Bump the version of scala-json to 1.1.1.

## v3.0.0 - 2018-12-03

This release removes the dependency on Guice and the ability to use these classes
with dependency injection.

It also removes deprecated methods.

## v2.8.0 - 2018-12-03

This release adds support for storing instances of `java.net.URI` with the implicit
Scanamo formats.

## v2.7.0 - 2018-11-14

This release adds some extra helpers to `LocalVersionedHybridStore` that don't
require passing an unnecessary `bucket: Bucket` parameter.

## v2.6.0 - 2018-11-08

Adds a new class `VHSIndexEntry[M](hybridRecord: HybridRecord, metadata: M)` which is
returned as the result of a call to `updateRecord`.

Additionally, fixes a bug where `updateRecord` would return incorrect metadata when
updating an existing record.

## v2.5.0 - 2018-11-06

This patch adds three new helpers to the test fixtures:

```scala
trait S3 {
  def createS3ConfigWith(bucket: Bucket): S3Config
}

trait LocalDynamoDB {
  def createDynamoConfigWith(table: Table): DynamoConfig
}

trait LocalVersionedHybridStore {
  def createVHSConfigWith(table: Table, bucket: Bucket, globalS3Prefix: String): VHSConfig
}
```

## v2.4.1 - 2018-10-16

Use `blocking` when necessary and don't create a new thread to close the stream in `ObjectStore`

## v2.4.0 - 2018-10-05

Upgrade scanamo version

## v2.3.0 - 2018-09-20

Close the input stream when retrieving an object with an ObjectStore

## v2.2.0 - 2018-09-03

When calling `VersionedDao.getRecord`, a `DynamoThroughputExceededException`
is wrapped as a `DynamoNonFatalError` in the same was as `updateRecord`.

## v2.1.0 - 2018-08-24

This adds a new helper to the S3 fixture:

```scala
def getObjectFromS3[T](location: ObjectLocation)(implicit decoder: Decoder[T]): T
```

## v2.0.0 - 2018-08-20

HybridRecord (the internal model used in the VHS) now stores the S3 bucket
name as well as the key.  This will break existing instances of the VHS.

There are also some new helpers on `LocalVersionedHybriStore`.

## v1.7.0 - 2018-08-20

VersionedDao: updateRecord[T] now returns the instance of T that was stored
VersionedHybridStore: updateRecord returns the store HybridRecord and Metadata

## v1.6.1 - 2018-08-01

This is an internal refactoring to use the new
[scala-json](https://github.com/wellcometrust/scala-json) library rather than
vendoring our JSON-related helpers.  No change to the external API.

## v1.6.0 - 2018-07-31

This patch removes the following test helpers, which were vendored from
the main repo and never intended to be part of the public storage API:

*   uk.ac.wellcome.storage.fixtures.Akka
*   uk.ac.wellcome.storage.utils.ExtendedPatience

## v1.5.2 - 2018-07-26

This is a no-op release that changes our sbt build settings.

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
