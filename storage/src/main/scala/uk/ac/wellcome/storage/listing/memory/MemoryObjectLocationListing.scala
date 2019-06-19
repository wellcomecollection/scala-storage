package uk.ac.wellcome.storage.listing.memory

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.listing.ObjectLocationListing
import uk.ac.wellcome.storage.store.memory.MemoryStoreBase
import uk.ac.wellcome.storage.{ListingFailure, ObjectLocation, ObjectLocationPrefix}

trait MemoryObjectLocationListing[T]
  extends ObjectLocationListing
    with Logging
    with MemoryStoreBase[ObjectLocation, T] {

  override def list(
    prefix: ObjectLocationPrefix
  ): Either[ListingFailure[ObjectLocation], Iterable[ObjectLocation]] = {

    // TODO: This should probably be startsWith
    val filtered = entries.filter {
      case (location, _) =>
        location.namespace == prefix.namespace && location.path.contains(prefix.path)
    }

    Right(filtered.map { case (location, _) => location })
  }
}
