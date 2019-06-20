package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.listing.memory.MemoryListing
import uk.ac.wellcome.storage.transfer.PrefixTransfer

trait MemoryPrefixTransfer[Ident, Prefix, T] extends PrefixTransfer[Prefix, Ident] {
  implicit val transfer: MemoryTransfer[Ident, T]
  implicit val listing: MemoryListing[Ident, Prefix, T]
}
