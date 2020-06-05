package uk.ac.wellcome.storage.tags

import uk.ac.wellcome.storage.{ReadError, WriteError}

trait Tags[Ident] {
  def get(id: Ident): Either[ReadError, Map[String, String]]

  def put(id: Ident, tags: Map[String, String]): Either[WriteError, Map[String, String]]
}
