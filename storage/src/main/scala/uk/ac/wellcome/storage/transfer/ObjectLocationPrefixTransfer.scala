package uk.ac.wellcome.storage.transfer

import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait ObjectLocationPrefixTransfer extends PrefixTransfer[ObjectLocationPrefix, ObjectLocation] {
  override protected def buildDstLocation(
    srcPrefix: ObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix,
    srcLocation: ObjectLocation): ObjectLocation =
    dstPrefix.asLocation(
      srcLocation.path.stripPrefix(srcPrefix.path)
    )
}
