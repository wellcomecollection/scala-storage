package uk.ac.wellcome.storage.maxima.memory

import uk.ac.wellcome.storage.{NoMaximaValueError, Version}
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.memory.MemoryStoreBase

trait MemoryMaxima[Id, T] extends Maxima[Id, Int] with MemoryStoreBase[Version[Id, Int], T] {
  def max(id: Id): Either[NoMaximaValueError, Int] = {
    val matchingEntries =
      entries
        .filter { case (ident, _) => ident.id == id }

    if (matchingEntries.isEmpty) {
      Left(NoMaximaValueError())
    } else {
      val (maxIdent, _) =
        matchingEntries.maxBy { case (ident, _) => ident.version }
      Right(maxIdent.version)
    }
  }
}
