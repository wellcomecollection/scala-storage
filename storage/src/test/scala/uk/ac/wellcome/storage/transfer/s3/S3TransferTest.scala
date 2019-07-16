package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.transfer.{TransferSourceFailure, TransferTestCases}

class S3TransferTest
  extends TransferTestCases[ObjectLocation, TypedStoreEntry[Record], Bucket, S3TypedStore[Record]]
    with S3TransferFixtures[Record]
    with RecordGenerators
    with BucketNamespaceFixtures {
  override def createSrcLocation(implicit bucket: Bucket): ObjectLocation = createId

  override def createDstLocation(implicit bucket: Bucket): ObjectLocation = createId

  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = Map.empty)

  // This test is intended to spot warnings from the SDK if we don't close
  // the dst inputStream correctly.
  it("errors if the destination exists but the source does not") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket)
      val dst = createObjectLocationWith(bucket)

      val initialEntries = Map(dst -> TypedStoreEntry(createRecord, metadata = Map.empty))

      withTransferStore(initialEntries) { implicit store =>
        withTransfer { transfer =>
          transfer.transfer(src, dst).left.value shouldBe a[TransferSourceFailure[_]]
        }
      }
    }
  }
}
