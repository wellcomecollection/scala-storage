package uk.ac.wellcome.storage.store.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.fixtures.StreamStoreFixtures
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait S3StreamStoreFixtures extends StreamStoreFixtures[S3ObjectLocation, S3StreamStore, Unit] with S3Fixtures {
  override def withStreamStoreImpl[R](
    context: Unit,
    initialEntries: Map[S3ObjectLocation, InputStreamWithLengthAndMetadata])(
    testWith: TestWith[S3StreamStore, R]): R = {
    initialEntries.foreach { case (location, stream) =>
      putStream(location, stream)
    }

    testWith(new S3StreamStore())
  }

  override def withStreamStoreContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())
}
