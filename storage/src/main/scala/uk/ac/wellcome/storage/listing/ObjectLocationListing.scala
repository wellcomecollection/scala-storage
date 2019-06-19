package uk.ac.wellcome.storage.listing

import uk.ac.wellcome.storage.{ListingFailure, ObjectLocation, ObjectLocationPrefix}

trait ObjectLocationListing {
  type ListingResult = Either[ListingFailure[ObjectLocation], Iterable[ObjectLocation]]

  def list(prefix: ObjectLocationPrefix): ListingResult
}
