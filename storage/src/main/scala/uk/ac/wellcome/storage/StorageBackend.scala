package uk.ac.wellcome.storage

import java.io.InputStream

trait StorageBackend {
  type PutResult = Either[BackendError with WriteError, Unit]
  type GetResult = Either[BackendError with ReadError, InputStream]

  def put(location: ObjectLocation,
          input: InputStream,
          metadata: Map[String, String]): PutResult
  def get(location: ObjectLocation): GetResult
}
