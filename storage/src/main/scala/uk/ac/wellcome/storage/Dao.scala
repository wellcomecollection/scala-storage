package uk.ac.wellcome.storage

trait Dao[Ident, T] {
  type GetResult = Either[ReadError with DaoError, T]
  type PutResult = Either[WriteError with DaoError, Unit]

  def get(id: Ident): GetResult
  def put(t: T): PutResult
}
