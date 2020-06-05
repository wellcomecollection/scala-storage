package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.s3.{S3TypedStore, S3TypedStoreFixtures}
import uk.ac.wellcome.storage.transfer.Transfer
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait S3TransferFixtures[T]
    extends TransferFixtures[
      ObjectLocation,
      T,
      S3TypedStore[T]]
    with S3TypedStoreFixtures[T] {
  override def withTransferStore[R](
    initialEntries: Map[ObjectLocation, T])(
    testWith: TestWith[S3TypedStore[T], R]): R =
    withTypedStoreImpl(storeContext = (), initialEntries = initialEntries) {
      typedStore =>
        testWith(typedStore)
    }

  override def withTransfer[R](
    testWith: TestWith[Transfer[ObjectLocation], R])(
    implicit store: S3TypedStore[T]): R =
    testWith(new S3Transfer())
}
