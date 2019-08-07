package uk.ac.wellcome.storage.transfer

import uk.ac.wellcome.storage.listing.Listing

import scala.concurrent.{ExecutionContext, Future}

trait PrefixTransfer[Prefix, Location] {
  implicit val transfer: Transfer[Location]
  implicit val listing: Listing[Prefix, Location]

  implicit val ec: ExecutionContext

  protected def buildDstLocation(
    srcPrefix: Prefix,
    dstPrefix: Prefix,
    srcLocation: Location
  ): Location

  private def copyPrefix(
    iterator: Iterable[Location],
    srcPrefix: Prefix,
    dstPrefix: Prefix
  ): Future[Either[PrefixTransferFailure, PrefixTransferSuccess]] = {
    val futures = iterator.map { srcLocation =>
      Future {
        transfer.transfer(
          src = srcLocation,
          dst = buildDstLocation(
            srcPrefix = srcPrefix,
            dstPrefix = dstPrefix,
            srcLocation = srcLocation)
        )
      }
    }

    // TODO: This accumulates all the results in memory.
    // Can we cope with copying a very large prefix?
    Future.sequence(futures).map { results =>
      val failures = results.collect { case Left(error)     => error }
      val successes = results.collect { case Right(success) => success }

      Either.cond(
        test = failures.isEmpty,
        right = PrefixTransferSuccess(successes.toSeq),
        left = PrefixTransferFailure(failures.toSeq, successes.toSeq)
      )
    }
  }

  def transferPrefix(
    srcPrefix: Prefix,
    dstPrefix: Prefix
  ): Future[Either[TransferFailure, TransferSuccess]] = {
    listing.list(srcPrefix) match {
      case Left(error) =>
        Future.successful(
          Left(PrefixTransferListingFailure(srcPrefix, error.e))
        )

      case Right(iterable) =>
        copyPrefix(iterable, srcPrefix, dstPrefix)
    }
  }
}
