package uk.ac.wellcome.storage

trait Dao[Ident, T] {
  type DaoGetResult = Either[ReadError with DaoError, T]
  type DaoPutResult = Either[WriteError with DaoError, Unit]

  def get(id: Ident): DaoGetResult
  def put(t: T): DaoPutResult
}
