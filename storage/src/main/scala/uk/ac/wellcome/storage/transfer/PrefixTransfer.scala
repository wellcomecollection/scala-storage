package uk.ac.wellcome.storage.transfer

import uk.ac.wellcome.storage.listing.Listing

trait PrefixTransfer[Prefix, Location] extends Transfer[Prefix] {
  implicit val transfer: Transfer[Location]
  implicit val listing: Listing[Prefix, Location]

  protected def buildDstLocation(
    srcPrefix: Prefix,
    dstPrefix: Prefix,
    srcLocation: Location
  ): Location

  private def copyPrefix(
    iterator: Iterable[Location],
    srcPrefix: Prefix,
    dstPrefix: Prefix
  ): Either[PrefixTransferFailure, PrefixTransferSuccess] = {

    val results = iterator.map { srcLocation =>
      transfer.transfer(
        src = srcLocation,
        dst = buildDstLocation(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix,
          srcLocation = srcLocation)
      )
    }

    // TODO: This accumulates all the results in memory.
    // Can we cope with copying a very large prefix?

    val failures = results.collect { case Left(error) => error }
    val successes = results.collect { case Right(success) => success }

    Either.cond(
      test = failures.isEmpty,
      right = PrefixTransferSuccess(successes.toSeq),
      left = PrefixTransferFailure(failures.toSeq, successes.toSeq)
    )
  }

  override def transfer(
    srcPrefix: Prefix,
    dstPrefix: Prefix
  ): Either[TransferFailure, TransferSuccess] = {
    for {
      iterable <- listing.list(srcPrefix) match {
        case Left(error) => Left(PrefixTransferListingFailure(srcPrefix, error.e))
        case Right(iterable) => Right(iterable)
      }
      result <- copyPrefix(iterable, srcPrefix, dstPrefix)
    } yield result
  }
}
