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

sealed trait TransferSuccess[Location] extends TransferResult {
  val source: Location
  val destination: Location
}

case class TransferNoOp[Location](source: Location, destination: Location)
    extends TransferSuccess[Location]

case class TransferPerformed[Location](source: Location, destination: Location)
    extends TransferSuccess[Location]
