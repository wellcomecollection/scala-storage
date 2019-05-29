package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.{ObjectLocation, PrefixCopier, PrefixCopierResult, StorageError}
import uk.ac.wellcome.storage.streaming.EncoderInstances._

class MemoryPrefixCopier(
  backend: MemoryStorageBackend
) extends PrefixCopier {
  override def copyObjects(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation): Either[StorageError, PrefixCopierResult] = {
    val srcObjects = backend.storage.filter { case (loc, _) =>
      loc.namespace == srcLocationPrefix.namespace &&
        loc.key.startsWith(srcLocationPrefix.key)
    }

    srcObjects.foreach { case (srcLocation, storedStream) =>
      val dstLocation = buildDstLocation(
        srcLocationPrefix = srcLocationPrefix,
        dstLocationPrefix = dstLocationPrefix,
        srcLocation = srcLocation
      )

      backend.put(
        location = dstLocation,
        inputStream = stringEncoder.toStream(storedStream.s).right.get,
        metadata = storedStream.metadata
      )
    }

    Right(PrefixCopierResult(srcObjects.size))
  }
}
