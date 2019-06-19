package uk.ac.wellcome.storage.listing.memory

import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.store.memory.MemoryStoreBase

trait MemoryListing[Ident, Prefix, T]
  extends Listing[Prefix, Ident]
    with MemoryStoreBase[Ident, T] {

  override def list(prefix: Prefix): ListingResult = {
    val matchingIdentifiers = entries
      .filter { case (ident, _) => startsWith(ident, prefix) }
      .map { case (ident, _) => ident }

    Right(matchingIdentifiers)
  }

  protected def startsWith(id: Ident, prefix: Prefix): Boolean
}
