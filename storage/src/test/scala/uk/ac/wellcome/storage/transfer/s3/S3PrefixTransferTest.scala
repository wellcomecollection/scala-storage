package uk.ac.wellcome.storage.transfer.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.listing.s3.{S3ObjectLocationListing, S3ObjectSummaryListing}
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore, S3TypedStoreFixtures}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.transfer._

class S3PrefixTransferTest
  extends PrefixTransferTestCases[S3ObjectLocation, S3ObjectLocationPrefix, Bucket, TypedStoreEntry[Record], S3TypedStore[Record]]
    with BucketNamespaceFixtures
    with MetadataGenerators
    with RecordGenerators
    with S3TypedStoreFixtures[Record] {
  override def withPrefixTransferStore[R](
    initialEntries: Map[S3ObjectLocation, TypedStoreEntry[Record]])(testWith: TestWith[S3TypedStore[Record], R]): R = {
    val streamStore = new S3StreamStore()

    withTypedStore(streamStore, initialEntries) { typedStore =>
      testWith(typedStore)
    }
  }

  override def withPrefixTransfer[R](testWith: TestWith[PrefixTransferImpl, R])(implicit store: S3TypedStore[Record]): R =
    testWith(S3PrefixTransfer())

  override def withExtraListingTransfer[R](testWith: TestWith[PrefixTransferImpl, R])(implicit store: S3TypedStore[Record]): R = {
    implicit val summaryListing: S3ObjectSummaryListing = new S3ObjectSummaryListing()
    implicit val listing: S3ObjectLocationListing = new S3ObjectLocationListing() {
      override def list(prefix: S3ObjectLocationPrefix): ListingResult =
        super.list(prefix).map { _ ++ Seq(createS3ObjectLocation) }
    }

    implicit val transfer: S3Transfer = new S3Transfer()

    testWith(new S3PrefixTransfer())
  }

  override def withBrokenListingTransfer[R](testWith: TestWith[PrefixTransferImpl, R])(implicit store: S3TypedStore[Record]): R = {
    implicit val summaryListing: S3ObjectSummaryListing = new S3ObjectSummaryListing()
    implicit val listing: S3ObjectLocationListing = new S3ObjectLocationListing() {
      override def list(prefix: S3ObjectLocationPrefix): ListingResult =
        Left(ListingFailure(prefix))
    }

    implicit val transfer: S3Transfer = new S3Transfer()

    testWith(new S3PrefixTransfer())
  }

  override def withBrokenTransfer[R](testWith: TestWith[PrefixTransferImpl, R])(implicit store: S3TypedStore[Record]): R = {
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    implicit val transfer: S3Transfer = new S3Transfer() {
      override def transfer(src: S3ObjectLocation, dst: S3ObjectLocation): Either[TransferFailure, TransferSuccess] =
        Left(TransferSourceFailure(src, dst))
    }

    testWith(new S3PrefixTransfer())
  }

  override def createPrefix(implicit bucket: Bucket): S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(bucket)

  override def createLocationFrom(prefix: S3ObjectLocationPrefix, suffix: String): S3ObjectLocation =
    prefix.asLocation(suffix)

  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = createValidMetadata)
}
