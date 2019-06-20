package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.TransferTestCases

class S3TransferTest extends TransferTestCases[ObjectLocation, TypedStoreEntry[Record], S3TypedStore[Record], S3Transfer, Bucket] with S3TransferFixtures[Record] with RecordGenerators {
  override def createSrcLocation(implicit bucket: Bucket): ObjectLocation = createObjectLocationWith(bucket.name)

  override def createDstLocation(implicit bucket: Bucket): ObjectLocation = createObjectLocationWith(bucket.name)

  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = Map.empty)
}
