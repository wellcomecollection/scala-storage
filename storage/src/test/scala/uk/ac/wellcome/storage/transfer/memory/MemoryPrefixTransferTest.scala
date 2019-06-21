package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ListingFailure
import uk.ac.wellcome.storage.listing.memory.MemoryListingFixtures
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer._

class MemoryPrefixTransferTest extends
  PrefixTransferTestCases[String, String, String, Array[Byte], MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]]
  with MemoryTransferFixtures[String, Array[Byte]] with StringNamespaceFixtures {
  override def createPrefix(implicit namespace: String): String = createId

  override def createLocationFrom(prefix: String, suffix: String): String = prefix + "/" + suffix

  override def createT: Array[Byte] = randomBytes()

  class InnerMemoryPrefixTransfer(initialEntries: Map[String, Array[Byte]]) extends MemoryStore[String, Array[Byte]](initialEntries) with MemoryPrefixTransfer[String, String, Array[Byte]] {
    override protected def startsWith(id: String, prefix: String): Boolean = id.startsWith(prefix)

    override protected def buildDstLocation(srcPrefix: String, dstPrefix: String, srcLocation: String): String =
      srcLocation.replaceAll("^" + srcPrefix, dstPrefix)
  }

  override def withPrefixTransferStore[R](initialEntries: Map[String, Array[Byte]])(testWith: TestWith[MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]], R]): R =
    testWith(new InnerMemoryPrefixTransfer(initialEntries))

  override def withPrefixTransfer[R](
                                      testWith: TestWith[PrefixTransfer[String, String], R])(
                                      implicit store: MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]): R =
    testWith(store)

  override def withExtraListingTransfer[R](testWith: TestWith[PrefixTransfer[String, String], R])(implicit store: MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]): R =
    testWith(
      new InnerMemoryPrefixTransfer(store.entries) {
        override def list(prefix: String): ListingResult = {
          val matchingIdentifiers = entries
            .filter { case (ident, _) => startsWith(ident, prefix) }
            .map { case (ident, _) => ident }

          Right(matchingIdentifiers ++ Seq(randomAlphanumeric))
        }
      }
    )

  override def withBrokenListingTransfer[R](testWith: TestWith[PrefixTransfer[String, String], R])(implicit store: MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]): R =
    testWith(
      new InnerMemoryPrefixTransfer(store.entries) {
        override def list(prefix: String): ListingResult =
          Left(ListingFailure(prefix))
      }
    )

  override def withBrokenTransfer[R](testWith: TestWith[PrefixTransfer[String, String], R])(implicit store: MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]): R =
    testWith(
      new InnerMemoryPrefixTransfer(store.entries) {
        override def transfer(src: String, dst: String): Either[TransferFailure, TransferSuccess] =
          Left(TransferSourceFailure(src, dst))
      }
    )
}
