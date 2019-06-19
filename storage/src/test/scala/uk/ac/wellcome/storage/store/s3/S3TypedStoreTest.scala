package uk.ac.wellcome.storage.store.s3

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.{TypedStoreEntry, TypedStoreTestCases}
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

import scala.util.Random

class S3TypedStoreTest extends TypedStoreTestCases[ObjectLocation, Record, Bucket, S3StreamStore, S3TypedStore[Record], Unit] with S3TypedStoreFixtures[Record] with MetadataGenerators with RecordGenerators with BucketNamespaceFixtures {
  override def withBrokenStreamStore[R](testWith: TestWith[S3StreamStore, R]): R = {
    val brokenS3StreamStore = new S3StreamStore {
      override def get(location: ObjectLocation): ReadEither = Left(
        StoreReadError(new Throwable("get: BOOM!"))
      )

      override def put(location: ObjectLocation)(inputStream: InputStreamWithLengthAndMetadata): WriteEither = Left(
        StoreWriteError(
          new Throwable("put: BOOM!")
        )
      )
    }

    testWith(brokenS3StreamStore)
  }

  override def withSingleValueStreamStore[R](rawStream: InputStream)(testWith: TestWith[S3StreamStore, R]): R = {
    val s3StreamStore: S3StreamStore = new S3StreamStore() {
      override def get(location: ObjectLocation): ReadEither =
        Right(
          Identified(location, new InputStreamWithLengthAndMetadata(rawStream, length = 0, metadata = Map.empty))
        )
    }

    testWith(s3StreamStore)
  }

  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = createValidMetadata)

  it("errors when given metadata that cannot be stored in S3") {
    withLocalS3Bucket { bucket =>
      withStoreImpl(initialEntries = Map.empty) { store: StoreImpl =>
        val location = createId(bucket)
        val entry = createT

        // The S3 API will only accept metadata strings
        // that can be represented in US-ASCII
        // See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#UserMetadata
        val invalidMetadata =
          (1 to 10)
            .map { _ =>
              (
                Random.nextString(8),
                Random.nextString(8)
              )
            }
            .toMap

        val invalidEntry = entry.copy(metadata = invalidMetadata)
        val result = store.put(location)(invalidEntry)

        result.left.value shouldBe a[MetadataCoercionFailure]
      }
    }
  }
}
