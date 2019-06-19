package uk.ac.wellcome.storage.listing

import uk.ac.wellcome.storage.{ListingFailure, ObjectLocation, ObjectLocationPrefix}

trait ObjectLocationListing {
  def list(prefix: ObjectLocationPrefix): Either[ListingFailure[ObjectLocation], Iterable[ObjectLocation]]
}
