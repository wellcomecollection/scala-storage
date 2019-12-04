package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.model.StorageClass
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{
  MetadataGenerators,
  Record,
  RecordGenerators
}
import uk.ac.wellcome.storage.listing.s3.{
  S3ObjectLocationListing,
  S3ObjectSummaryListing
}
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.{
  S3StreamStore,
  S3TypedStore,
  S3TypedStoreFixtures
}
import uk.ac.wellcome.storage.{
  ListingFailure,
  ObjectLocation,
  ObjectLocationPrefix
}
import uk.ac.wellcome.storage.transfer._

class S3PrefixTransferTest
    extends PrefixTransferTestCases[
      ObjectLocation,
      ObjectLocationPrefix,
      Bucket,
      TypedStoreEntry[Record],
      S3TypedStore[Record]]
    with BucketNamespaceFixtures
    with MetadataGenerators
    with RecordGenerators
    with S3TypedStoreFixtures[Record] {
  override def withPrefixTransferStore[R](
    initialEntries: Map[ObjectLocation, TypedStoreEntry[Record]])(
    testWith: TestWith[S3TypedStore[Record], R]): R = {
    val streamStore = new S3StreamStore()

    withTypedStore(streamStore, initialEntries) { typedStore =>
      testWith(typedStore)
    }
  }

  override def withPrefixTransfer[R](
    testWith: TestWith[PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
                       R])(implicit store: S3TypedStore[Record]): R =
    testWith(S3PrefixTransfer())

  override def withExtraListingTransfer[R](
    testWith: TestWith[PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
                       R])(implicit store: S3TypedStore[Record]): R = {
    implicit val summaryListing: S3ObjectSummaryListing =
      new S3ObjectSummaryListing()
    implicit val listing: S3ObjectLocationListing =
      new S3ObjectLocationListing() {
        override def list(prefix: ObjectLocationPrefix): ListingResult =
          super.list(prefix).map { _ ++ Seq(createObjectLocation) }
      }

    implicit val transfer: S3Transfer = new S3Transfer()

    testWith(new S3PrefixTransfer())
  }

  override def withBrokenListingTransfer[R](
    testWith: TestWith[PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
                       R])(implicit store: S3TypedStore[Record]): R = {
    implicit val summaryListing: S3ObjectSummaryListing =
      new S3ObjectSummaryListing()
    implicit val listing: S3ObjectLocationListing =
      new S3ObjectLocationListing() {
        override def list(prefix: ObjectLocationPrefix): ListingResult =
          Left(ListingFailure(prefix))
      }

    implicit val transfer: S3Transfer = new S3Transfer()

    testWith(new S3PrefixTransfer())
  }

  override def withBrokenTransfer[R](
    testWith: TestWith[PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
                       R])(implicit store: S3TypedStore[Record]): R = {
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    implicit val transfer: S3Transfer = new S3Transfer() {
      override def transfer(
        src: ObjectLocation,
        dst: ObjectLocation): Either[TransferFailure, TransferSuccess] =
        Left(TransferSourceFailure(src, dst))
    }

    testWith(new S3PrefixTransfer())
  }

  override def createPrefix(implicit bucket: Bucket): ObjectLocationPrefix =
    createObjectLocationWith(bucket).asPrefix

  override def createLocationFrom(prefix: ObjectLocationPrefix,
                                  suffix: String): ObjectLocation =
    prefix.asLocation(suffix)

  override def createT: TypedStoreEntry[Record] =
    TypedStoreEntry(createRecord, metadata = createValidMetadata)

  it("uses the specified storage class for the copied object") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val src = createObjectLocationWith(srcBucket)
        val dst = ObjectLocation(namespace = dstBucket.name, path = src.path)

        val contents = randomAlphanumeric
        s3Client.putObject(src.namespace, src.path, contents)

        val prefixTransfer = S3PrefixTransfer(
          storageClass = StorageClass.OneZoneInfrequentAccess
        )

        val srcPrefix = ObjectLocationPrefix(namespace = srcBucket.name, path = "")
        val dstPrefix = ObjectLocationPrefix(namespace = dstBucket.name, path = "")

        prefixTransfer.transferPrefix(srcPrefix, dstPrefix) shouldBe a[Right[_, _]]

        s3Client.getObjectMetadata(dst.namespace, dst.path)
          .getStorageClass shouldBe StorageClass.OneZoneInfrequentAccess.toString
      }
    }
  }
}
