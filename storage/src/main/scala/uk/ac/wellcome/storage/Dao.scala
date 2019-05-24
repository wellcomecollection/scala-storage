package uk.ac.wellcome.storage

trait Dao[Ident, T] {
  def get(id: Ident): Either[ReadError with DaoError, T]
  def put(t: T): Either[WriteError with DaoError, Unit]
}
