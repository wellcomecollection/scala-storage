# CHANGELOG

## v7.14.0 - 2019-06-25

Adds a DynamoHybridStore

## v7.13.1 - 2019-06-24

Improve the tests for DynamoStore.

## v7.13.0 - 2019-06-24

Adds the VersionedStore with in-memory & DynamoDB implementations.

## v7.12.1 - 2019-06-24

Makes MemoryLockDao thread-safe.

## v7.12.0 - 2019-06-22

Adds the `PrefixTransfer` trait, along with in-memory and S3 implementations.

## v7.11.0 - 2019-06-19

This adds a `Listing` trait, for finding instances of something in a store, along with in-memory and S3 implementations.

## v7.10.0 - 2019-06-19

This adds a `Transfer` trait, for coyping objects between stores, including memory and S3 implementations.

## v7.9.0 - 2019-06-19

This adds an S3 implementation of TypedStore.

## v7.8.0 - 2019-06-18

This adds the `TypedStore` class.

## v7.7.0 - 2019-06-18

Adds the S3 implementation of a StreamStore.

## v7.6.0 - 2019-06-18

-   Remove the codec, decoder and encoder for `java.io.InputStream`.

-   The decoder is more relaxed about the sort of InputStream it takes as input.
    You get reduced error checking if you don't pass a length, but that's all.

-   Add the initial `Store` and `StreamingStore` implementations.

-   Separate our the notion of `HasLength` and `HasMetadata` for instances of InputStream.

## v7.5.0 - 2019-06-17

Add a codec, decoder and encoder for `Array[Byte]`.

## v7.4.1 - 2019-06-17

Improve the testing of our custom DynamoFormat implementations, and fix a bug in the DynamoFormat for `java.net.URI`.

## v7.4.0 - 2019-06-17

This patch adds the `Maxima` class, for looking up the highest version of a thing.  It includes in-memory and DynamoDB implementations.

## v7.3.0 - 2019-06-17

This rearranges the classes around locking to follow a more consistent pattern, and improves the testing of the locking classes.

## v7.2.0 - 2019-06-17

This adds the `FiniteInputStream` class, an instance of `java.io.InputStream` that records the length of the underlying stream.

## v7.1.0 - 2019-06-17

Bump the bundled version of Scanamo and expand the tests around DynamoFormat.

## v7.0.0 - 2019-06-17

This deletes most of the existing scala-storage functionality in prep for a major refactor.

## v6.1.3 - 2019-06-03

Remove an unused dependency on scala-monitoring.

## v6.1.2 - 2019-05-31

Remove internal ConditionalUpdateDao

## v6.1.1 - 2019-05-31

Make it clear to the type system that the backend of a
MemoryObjectStore is a MemoryStorageBackend.

## v6.1.0 - 2019-05-29

A bunch of fixes that came from integrating the new library into the storage-service repo:

*   Add an S3-agnostic generator for random instances of `ObjectLocation`
*   Add an underlying trait `PrefixCopier` that has two implementations: `S3PrefixCopier` and `MemoryPrefixCopier`.
    The latter is suitable for use in tests.
*   Make it easier to define DynamoDB tables with the `LocalDynamoDB` fixture.

## v6.0.1 - 2019-05-28

*   In the VersionedHybridStore, when updating, don't write to the ObjectStore unless the underlying object has actually changed.

*   Make it easier to access underlying entries in MemoryVersionedDao

*   Add a way to delete objects from MemoryObjectStore

## v6.0.0 - 2019-05-28

This release modifies most of the data store methods to return `Either[Error, T]` instead of `Try[Option[T]]`, so we can more easily distinguish between the error cases "doesn't exist" and "other error".

## v5.0.0 - 2019-05-20

This release adds a bunch of new type classes for database access, which should make mocking instances of VersionedDao easier in tests.

The new hierarchy is:

```scala
// Basic database access object
trait Dao[Ident, T] {
  def get(id: Ident): Try[Option[T]]
  def put(t: T): Try[T]
}

// Put conditions on the put() operation
trait ConditionalUpdateDao[Ident, T] extends Dao[Ident, T]

// The traditional VersionedDao
trait VersionedDao[T] {
  implicit val underlying: ConditionalUpdateDao[String, T]

  def get(id: String): Try[Option[T]] = underlying.get(id)

  def put(value: T): Try[T]
}

// A new VHS
trait VersionedHybridStore[Ident, T, Metadata] {
  def update: Try[Entry[Ident, Metadata]]
  def get: Try[Option[T]]
}
```

## v4.7.0 - 2019-05-15

This modifies the StorageBackend and ObjectStore traits to use `Try` instead of `Future`.

It also pushes more of the logic inside ObjectStore itself, is better tested, and should make it a bit easier to write tests that require an ObjectStore.

## v4.6.0 - 2019-05-15

DynamoHashKeyLookup now returns instances of Try, not Future.

## v4.5.0 - 2019-05-13

Add a new class `DynamoHashKeyLookup` for looking up the record with the highest/lowest value of a range key.

## v4.4.0 - 2019-05-10

Add a new trait for database objects that manage versioning:

```scala
trait VersionedDao[T] {
  def put(value: T): Try[T]
  def get(id: String): Try[Option[T]]
}
```

See `InMemoryVersionedDao` for an example implementation.

The previous VersionedDao (now DynamoVersionedDao) is another implementation of this trait.

## v4.3.0 - 2019-05-09

Add a decoder/encoder for s3:// URIs.

## v4.2.0 - 2019-05-03

Bump version of scala-monitoring to 2.2.0.

## v4.1.0 - 2019-05-02

*   Add type parameters to `DynamoLockingService`
*   Record a lock history on `InMemoryLockDao` for use in tests

## v4.0.0 - 2019-05-02

This is a major change to the way we do locking.

We now provide generic type classes `LockDao` and `LockingService`, and
concrete implementations as `DynamoLockDao` and `DynamoLockingService`.

## v3.9.0 - 2019-04-26

Add a `join()` method to `ObjectLocation` that lets you append to the key.

## v3.8.0 - 2019-04-25

Add a Typesafe builder for the VersionedDao.

## v3.7.2 - 2019-04-24

Reduce the number of streams we open and discard in `S3Copier`.

## v3.7.1 - 2019-04-24

Close the input streams we use to compare objects in `S3Copier`.

## v3.7.0 - 2019-04-16

Modify `S3Copier` so that if the destination object exists and is the same as the source object, we skip the CopyObject operation.

If the destination object exists and is different, `S3Copier` throws an error.

## v3.6.2 - 2019-04-05

Bump the version of scala-monitoring.

## v3.6.1 - 2019-04-04

valid buckets created in test fixtures (lowercase characters in bucket name)

## v3.6.0 - 2019-03-15

This adds some utilities for locking operations with DynamoDB, extracted from the matcher in the catalogue pipeline.

## v3.5.2 - 2019-03-13

have S3PrefixOperator return a S3PrefixCopierResult with the number of files copied.

## v3.5.1 - 2019-03-07

Use Java NIO Paths to make path generation behaviour predictable

## v3.5.0 - 2019-03-05

Add some convenience methods for creating instances of `S3PrefixCopier`.

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
