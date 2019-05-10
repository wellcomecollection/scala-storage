package uk.ac.wellcome.storage

import java.io.InputStream

import scala.concurrent.Future
import scala.util.Try

trait BetterStorageBackend {
  def put(location: ObjectLocation,
          input: InputStream,
          metadata: Map[String, String]): Try[Unit]
  def get(location: ObjectLocation): Try[InputStream]
}

trait StorageBackend {
  def put(location: ObjectLocation,
          input: InputStream,
          metadata: Map[String, String]): Future[Unit]
  def get(location: ObjectLocation): Future[InputStream]
}
