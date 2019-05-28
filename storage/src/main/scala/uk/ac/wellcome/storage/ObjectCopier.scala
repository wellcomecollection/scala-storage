package uk.ac.wellcome.storage

trait ObjectCopier {
  def copy(src: ObjectLocation, dst: ObjectLocation): Either[StorageError, Unit]
}
