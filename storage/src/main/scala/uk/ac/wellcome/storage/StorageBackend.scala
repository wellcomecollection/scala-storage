package uk.ac.wellcome.storage

import java.io.InputStream

import scala.util.Try

trait StorageBackend {
  def put(location: ObjectLocation,
          input: InputStream,
          metadata: Map[String, String]): Try[Unit]
  def get(location: ObjectLocation): Try[InputStream]
}
