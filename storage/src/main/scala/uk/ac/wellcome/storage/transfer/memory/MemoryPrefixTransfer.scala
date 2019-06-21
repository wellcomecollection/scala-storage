package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.listing.memory.MemoryListing
import uk.ac.wellcome.storage.transfer.PrefixTransfer

trait MemoryPrefixTransfer[Ident, Prefix, T] extends PrefixTransfer[Prefix, Ident] with MemoryTransfer[Ident, T] with MemoryListing[Ident, Prefix, T] {
  implicit val transfer: MemoryTransfer[Ident, T] = this
  implicit val listing: MemoryListing[Ident, Prefix, T] = this
}
