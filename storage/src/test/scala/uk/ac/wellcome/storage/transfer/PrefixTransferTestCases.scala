package uk.ac.wellcome.storage.transfer

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.listing.memory.MemoryListingFixtures
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.fixtures.{NamespaceFixtures, StringNamespaceFixtures}
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.memory.{MemoryPrefixTransfer, MemoryTransferFixtures}

trait PrefixTransferFixtures[Location, Prefix, T, StoreImpl <: Store[Location, T]] {
  def withPrefixTransferStore[R](initialEntries: Map[Location, T])(testWith: TestWith[StoreImpl, R]): R

  def withPrefixTransfer[R](testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit store: StoreImpl): R
}

trait PrefixTransferTestCases[Location, Prefix, Namespace, T, StoreImpl <: Store[Location, T]] extends FunSpec with Matchers with PrefixTransferFixtures[Location, Prefix, T, StoreImpl] with EitherValues with NamespaceFixtures[Location, Namespace] {
  def createPrefix(implicit namespace: Namespace): Prefix

  def createLocationFrom(prefix: Prefix, suffix: String): Location

  def createT: T

  it("does nothing if the prefix is empty") {
    withNamespace { implicit namespace =>
      withPrefixTransferStore(initialEntries = Map.empty) { implicit store =>
        withPrefixTransfer { prefixTransfer =>
          prefixTransfer.transferPrefix(
            srcPrefix = createPrefix,
            dstPrefix = createPrefix
          ).right.value shouldBe PrefixTransferSuccess(Seq.empty)
        }
      }
    }
  }

  it("copies a single item") {
    withNamespace { implicit namespace =>
      val srcPrefix = createPrefix
      val dstPrefix = createPrefix

      val srcLocation = createLocationFrom(srcPrefix, suffix = "1.txt")
      val dstLocation = createLocationFrom(dstPrefix, suffix = "1.txt")

      val t = createT

      withPrefixTransferStore(initialEntries = Map(srcLocation -> t)) { implicit store =>
        withPrefixTransfer { prefixTransfer =>
          prefixTransfer.transferPrefix(
            srcPrefix = srcPrefix,
            dstPrefix = dstPrefix
          ).right.value shouldBe PrefixTransferSuccess(
            Seq(TransferPerformed(srcLocation, dstLocation))
          )

          store.get(srcLocation).right.value shouldBe Identified(srcLocation, t)
          store.get(dstLocation).right.value shouldBe Identified(dstLocation, t)
        }
      }
    }
  }

  it("copies multiple items") {
    true shouldBe false
  }

  it("fails if a single item fails to copy") {
    true shouldBe false
  }

  it("fails if the underlying transfer is broken") {
    true shouldBe false
  }

  it("fails if the underlying listing is broken") {
    true shouldBe false
  }
}

class MemoryPrefixTransferTest extends
  PrefixTransferTestCases[String, String, String, Array[Byte], MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]]
with MemoryTransferFixtures[String, Array[Byte]] with StringNamespaceFixtures {
  override def createPrefix(implicit namespace: String): String = createId

  override def createLocationFrom(prefix: String, suffix: String): String = prefix + "/" + suffix

  override def createT: Array[Byte] = randomBytes()

  override def withPrefixTransferStore[R](initialEntries: Map[String, Array[Byte]])(testWith: TestWith[MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]], R]): R =
    testWith(
      new MemoryStore[String, Array[Byte]](initialEntries) with MemoryPrefixTransfer[String, String, Array[Byte]] {
        override protected def startsWith(id: String, prefix: String): Boolean = id.startsWith(prefix)

        override protected def buildDstLocation(srcPrefix: String, dstPrefix: String, srcLocation: String): String =
          srcLocation.replaceAll("^" + srcPrefix, dstPrefix)
      }
    )

  override def withPrefixTransfer[R](
    testWith: TestWith[PrefixTransfer[String, String], R])(
    implicit store: MemoryStore[String, Array[Byte]] with MemoryPrefixTransfer[String, String, Array[Byte]]): R =
    testWith(store)
}
