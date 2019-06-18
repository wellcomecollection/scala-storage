package uk.ac.wellcome.storage.store.s3

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.fixtures.{BucketNamespaceFixtures, TypedStoreFixtures}
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry, TypedStoreTestCases}
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLengthAndMetadata}

trait S3TypedStoreFixtures[T] extends TypedStoreFixtures[ObjectLocation, T, S3StreamStore, Unit] with S3StreamStoreFixtures {
  override def withTypedStore[R](streamStore: S3StreamStore, initialEntries: Map[ObjectLocation, TypedStoreEntry[T]])(testWith: TestWith[TypedStore[ObjectLocation, T], R])(implicit codec: Codec[T]): R = {
    implicit val s3StreamStore: S3StreamStore = streamStore

    initialEntries.map { case (location, entry) =>
      val stream = codec.toStream(entry.t).right.value

      val uploadStream = new InputStreamWithLengthAndMetadata(stream, stream.length, entry.metadata)

      putStream(location, uploadStream)
    }

    testWith(new S3TypedStore[T]())
  }
}

class S3TypedStoreTest extends TypedStoreTestCases[ObjectLocation, Record, Bucket, S3StreamStore, Unit] with S3TypedStoreFixtures[Record] with MetadataGenerators with RecordGenerators with BucketNamespaceFixtures {
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
}
