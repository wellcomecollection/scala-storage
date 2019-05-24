package uk.ac.wellcome.storage.memory

import java.io.InputStream

import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation, StorageBackend}
import uk.ac.wellcome.storage.streaming.CodecInstances._

class MemoryStorageBackend() extends StorageBackend {

  case class StoredStream(
    s: String,
    metadata: Map[String, String]
  )

  var storage: Map[ObjectLocation, StoredStream] = Map.empty

  override def put(location: ObjectLocation,
                   inputStream: InputStream,
                   metadata: Map[String, String]): PutResult = {
    storage = storage ++ Map(
      location -> StoredStream(
        s = stringCodec.fromStream(inputStream).right.get,
        metadata = metadata
      ))
    Right(())
  }

  override def get(location: ObjectLocation): GetResult =
    storage.get(location) match {
      case Some(storedStream) => Right(stringCodec.toStream(storedStream.s).right.get)
      case None => Left(DoesNotExistError(
        new Throwable(s"Nothing at $location")
      ))
    }
}
