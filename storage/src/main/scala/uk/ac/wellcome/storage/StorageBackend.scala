package uk.ac.wellcome.storage

import java.io.InputStream

trait StorageBackend {
  type PutResult = Either[WriteError, Unit]
  type GetResult = Either[ReadError, InputStream]

  def put(location: ObjectLocation,
          input: InputStream,
          metadata: Map[String, String]): PutResult
  def get(location: ObjectLocation): GetResult
}
