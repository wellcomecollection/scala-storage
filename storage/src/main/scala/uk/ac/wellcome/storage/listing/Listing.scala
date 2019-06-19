package uk.ac.wellcome.storage.listing

import uk.ac.wellcome.storage.ListingFailure

trait Listing[Prefix, Result] {
  type ListingResult = Either[ListingFailure[Prefix], Iterable[Result]]

  def list(prefix: Prefix): ListingResult
}
