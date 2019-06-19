package uk.ac.wellcome.storage.transfer

sealed trait TransferResult

sealed trait TransferFailure extends TransferResult {
  val e: Throwable
}

sealed trait TransferSuccess extends TransferResult
