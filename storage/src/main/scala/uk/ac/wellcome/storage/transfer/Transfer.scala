package uk.ac.wellcome.storage.transfer

trait Transfer[Source, Destination] {
  def transfer(src: Source, dst: Destination): Either[TransferFailure, TransferSuccess]
}
