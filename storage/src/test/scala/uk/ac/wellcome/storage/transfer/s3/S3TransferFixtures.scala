package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.s3.{S3TypedStore, S3TypedStoreFixtures}
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait S3TransferFixtures[T] extends TransferFixtures[ObjectLocation, TypedStoreEntry[T], S3TypedStore[T], S3Transfer, Bucket] with S3TypedStoreFixtures[T] {
  override def withTransferStoreContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withTransferStore[R](initialEntries: Map[ObjectLocation, TypedStoreEntry[T]])(testWith: TestWith[S3TypedStore[T], R])(implicit context: Bucket): R = {
    debug(context)  // otherwise the compiler complains the context is unused
    withTypedStoreImpl(storeContext = (), initialEntries = initialEntries) { typedStore =>
      testWith(typedStore)
    }
  }

  override def withTransfer[R](testWith: TestWith[S3Transfer, R])(implicit context: Bucket): R =
    testWith(new S3Transfer())
}
