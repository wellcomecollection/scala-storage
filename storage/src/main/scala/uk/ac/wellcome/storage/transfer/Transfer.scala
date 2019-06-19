package uk.ac.wellcome.storage.transfer

trait Transfer[Location] {
  def transfer(
    src: Location,
    dst: Location): Either[TransferFailure, TransferSuccess[Location]]
}
