package uk.ac.wellcome.storage

case class PrefixCopierResult(fileCount: Int)

trait PrefixCopier {
  def copyObjects(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation): Either[StorageError, PrefixCopierResult]
}
