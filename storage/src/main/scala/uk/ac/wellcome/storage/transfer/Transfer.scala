package uk.ac.wellcome.storage.transfer

trait Transfer[Location] {
  def transfer(src: Location,
               dst: Location,
               checkForExisting: Boolean = true): Either[TransferFailure, TransferSuccess] =
    if (checkForExisting) {
      transferWithCheckForExisting(src, dst)
    } else {
      transferWithOverwrites(src, dst)
    }

  protected def transferWithCheckForExisting(src: Location, dst: Location): Either[TransferFailure, TransferSuccess]

  protected def transferWithOverwrites(src: Location, dst: Location): Either[TransferFailure, TransferSuccess]
}
