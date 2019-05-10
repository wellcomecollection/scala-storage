package uk.ac.wellcome.storage.memory

import java.io.InputStream

import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.{ObjectLocation, StorageBackend}

import scala.io.Source
import scala.util.{Failure, Success, Try}

class MemoryStorageBackend() extends StorageBackend {
  case class StoredStream(
    s: String,
    metadata: Map[String, String]
  )

  var storage: Map[ObjectLocation, StoredStream] = Map.empty

  override def put(location: ObjectLocation,
                   input: InputStream,
                   metadata: Map[String, String]): Try[Unit] = Try {
    storage = storage ++ Map(
      location -> StoredStream(
        s = Source.fromInputStream(input).mkString,
        metadata = metadata
      ))
  }

  override def get(location: ObjectLocation): Try[InputStream] =
    storage.get(location) match {
      case Some(storedStream) =>
        Success(IOUtils.toInputStream(storedStream.s, "UTF-8"))
      case None => Failure(new Throwable(s"Nothing at $location"))
    }
}
