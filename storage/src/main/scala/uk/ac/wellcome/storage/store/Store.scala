package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage._

trait Store[Ident, T]
  extends Readable[Ident, T]
  with Writable[Ident, T]

trait Readable[Ident, T] {
  type ReadEither = Either[ReadError, Identified[Ident, T]]

  def get(id: Ident): ReadEither
}

trait Writable[Ident, T] {
  type WriteEither = Either[WriteError, Identified[Ident, T]]

  def put(id: Ident)(t: T): WriteEither
}
