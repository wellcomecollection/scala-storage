package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.s3.{S3TypedStore, S3TypedStoreFixtures}
import uk.ac.wellcome.storage.transfer.{Transfer, TransferTestCases}

class S3TransferTest extends TransferTestCases[ObjectLocation, TypedStoreEntry[Record], S3TypedStore[Record], Bucket] with S3TransferFixtures[Record] with S3TypedStoreFixtures[Record] with RecordGenerators {
  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = Map.empty)

  override def withTransferStore[R](initialEntries: Map[ObjectLocation, TypedStoreEntry[Record]])(testWith: TestWith[S3TypedStore[Record], R])(implicit context: Bucket): R = {
    debug(context)  // otherwise the compiler complains the context is unused
    withTypedStoreImpl(storeContext = (), initialEntries = initialEntries) { typedStore =>
      testWith(typedStore)
    }
  }

  override def withTransfer[R](testWith: TestWith[Transfer[ObjectLocation], R])(implicit context: Bucket): R =
    testWith(new S3Transfer())
}
