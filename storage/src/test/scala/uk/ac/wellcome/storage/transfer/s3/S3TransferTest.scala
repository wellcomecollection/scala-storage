package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.TransferTestCases

class S3TransferTest
  extends TransferTestCases[S3ObjectLocation, TypedStoreEntry[Record], Bucket, S3TypedStore[Record]]
    with S3TransferFixtures[Record]
    with RecordGenerators
    with BucketNamespaceFixtures {
  override def createSrcLocation(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def createDstLocation(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = Map.empty)
}
