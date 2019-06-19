package uk.ac.wellcome.storage.store.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}
import uk.ac.wellcome.storage.store.fixtures.TypedStoreFixtures
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
