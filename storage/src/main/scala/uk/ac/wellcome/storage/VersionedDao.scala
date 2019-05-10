package uk.ac.wellcome.storage

import scala.util.Try

trait VersionedDao[T] {
  def put(value: T): Try[T]
  def get(id: String): Try[Option[T]]
}
