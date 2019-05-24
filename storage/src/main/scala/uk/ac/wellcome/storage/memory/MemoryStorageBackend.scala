package uk.ac.wellcome.storage.memory

import java.io.InputStream

import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation, StorageBackend}

class MemoryStorageBackend() extends StorageBackend {

  case class StoredStream(
    inputStream: InputStream,
    metadata: Map[String, String]
  )

  var storage: Map[ObjectLocation, StoredStream] = Map.empty

  override def put(location: ObjectLocation,
                   inputStream: InputStream,
                   metadata: Map[String, String]): PutResult = {
    storage = storage ++ Map(
      location -> StoredStream(
        inputStream = inputStream,
        metadata = metadata
      ))
    Right(())
  }

  override def get(location: ObjectLocation): GetResult =
    storage.get(location) match {
      case Some(storedStream) => Right(storedStream.inputStream)
      case None => Left(DoesNotExistError(
        new Throwable(s"Nothing at $location")
      ))
    }
}
