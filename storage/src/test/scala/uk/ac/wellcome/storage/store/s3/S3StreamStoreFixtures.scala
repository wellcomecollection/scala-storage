package uk.ac.wellcome.storage.store.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.fixtures.StreamStoreFixtures
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait S3StreamStoreFixtures extends StreamStoreFixtures[ObjectLocation, Unit] with S3Fixtures {
  override def withStreamStoreImpl[R](context: Unit, initialEntries: Map[ObjectLocation, InputStreamWithLengthAndMetadata])(testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata], R]): R = {
    initialEntries.map { case (location, stream) =>
      putStream(location, stream)
    }

    testWith(new S3StreamingStore())
  }

  override def withStreamStoreContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())
}
