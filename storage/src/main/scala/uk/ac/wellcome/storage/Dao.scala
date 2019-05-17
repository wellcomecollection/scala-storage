package uk.ac.wellcome.storage

import scala.util.Try

trait Dao[Ident, T] {
  def get(id: Ident): Try[Option[T]]
  def put(t: T): Try[T]
}
