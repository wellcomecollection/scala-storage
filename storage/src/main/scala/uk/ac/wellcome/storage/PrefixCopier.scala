package uk.ac.wellcome.storage

case class PrefixCopierResult(fileCount: Int)

trait PrefixCopier {
  def copyObjects(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation): Either[StorageError, PrefixCopierResult]

  protected def buildDstLocation(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation,
    srcLocation: ObjectLocation
  ): ObjectLocation = {
    val relativeKey = srcLocation.key
      .stripPrefix(srcLocationPrefix.key)

    dstLocationPrefix.join(relativeKey)
  }
}
