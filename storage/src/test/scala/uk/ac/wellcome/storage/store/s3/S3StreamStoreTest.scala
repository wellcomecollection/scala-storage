package uk.ac.wellcome.storage.store.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class S3StreamStoreTest extends StreamStoreTestCases[ObjectLocation, Bucket, Unit] with S3Fixtures {
  override def withStoreImpl[R](
    storeContext: Unit,
    initialEntries: Map[ObjectLocation, InputStreamWithLengthAndMetadata])(
    testWith: TestWith[StoreImpl, R]): R = {
    initialEntries.map { case (location, stream) =>
      putStream(location, stream)
    }

    testWith(new S3StreamingStore())
  }

  override def withStoreContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createId(implicit namespace: Bucket): ObjectLocation =
    ObjectLocation(
      namespace = namespace.name,
      key = randomAlphanumeric
    )
}
