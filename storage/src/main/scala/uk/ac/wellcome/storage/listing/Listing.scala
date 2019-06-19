package uk.ac.wellcome.storage.listing

import uk.ac.wellcome.storage.ListingFailure

trait Listing[Ident, Prefix] {
  type ListingResult = Either[ListingFailure[Ident], Iterable[Ident]]

  def list(prefix: Prefix): ListingResult
}
