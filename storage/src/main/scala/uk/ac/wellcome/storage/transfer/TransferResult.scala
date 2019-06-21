package uk.ac.wellcome.storage.transfer

sealed trait TransferResult

sealed trait TransferFailure extends TransferResult {
  val e: Throwable
}

case class TransferSourceFailure[Location](source: Location,
                                           destination: Location,
                                           e: Throwable = new Error())
    extends TransferFailure

case class TransferDestinationFailure[Location](source: Location,
                                                destination: Location,
                                                e: Throwable = new Error())
    extends TransferFailure

case class TransferOverwriteFailure[Location](source: Location,
                                              destination: Location,
                                              e: Throwable = new Error())
    extends TransferFailure

case class PrefixTransferFailure(failures: Seq[TransferFailure],
                                 successes: Seq[TransferSuccess],
                                 e: Throwable = new Error())
    extends TransferFailure

case class PrefixTransferListingFailure[Prefix](prefix: Prefix,
                                                e: Throwable = new Error())
    extends TransferFailure

sealed trait TransferSuccess extends TransferResult

case class TransferNoOp[Location](source: Location, destination: Location)
    extends TransferSuccess

case class TransferPerformed[Location](source: Location, destination: Location)
    extends TransferSuccess

case class PrefixTransferSuccess(successes: Seq[TransferSuccess])
    extends TransferSuccess
