package uk.ac.wellcome.storage.maxima

import uk.ac.wellcome.storage.ReadError

trait Maxima[QueryParameter, Id] {
  def max(q: QueryParameter): Either[ReadError, Id]
}

