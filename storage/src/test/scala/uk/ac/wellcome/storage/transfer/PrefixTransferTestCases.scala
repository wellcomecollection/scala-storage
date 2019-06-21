package uk.ac.wellcome.storage.transfer

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{Identified, ListingFailure}
import uk.ac.wellcome.storage.listing.memory.MemoryListingFixtures
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.fixtures.{NamespaceFixtures, StringNamespaceFixtures}
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.memory.{MemoryPrefixTransfer, MemoryTransferFixtures}

trait PrefixTransferTestCases[Location, Prefix, Namespace, T, StoreImpl <: Store[Location, T]] extends FunSpec with Matchers with EitherValues with NamespaceFixtures[Location, Namespace] {
  def withPrefixTransferStore[R](initialEntries: Map[Location, T])(testWith: TestWith[StoreImpl, R]): R

  def withPrefixTransfer[R](testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit store: StoreImpl): R

  def withExtraListingTransfer[R](testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit store: StoreImpl): R
  def withBrokenListingTransfer[R](testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit store: StoreImpl): R
  def withBrokenTransfer[R](testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit store: StoreImpl): R

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
    withNamespace { implicit namespace =>
      val srcPrefix = createPrefix
      val dstPrefix = createPrefix

      val srcLocations = (1 to 5).map { i => createLocationFrom(srcPrefix, suffix = s"$i.txt") }
      val dstLocations = (1 to 5).map { i => createLocationFrom(dstPrefix, suffix = s"$i.txt") }

      val valuesT = (1 to 5).map { _ => createT }

      val expectedResults: Seq[TransferPerformed[Location]] = srcLocations.zip(dstLocations).map { case (src, dst) =>
        TransferPerformed(src, dst)
      }

      withPrefixTransferStore(initialEntries = srcLocations.zip(valuesT).toMap) { implicit store =>
        withPrefixTransfer { prefixTransfer =>
          val result = prefixTransfer.transferPrefix(
            srcPrefix = srcPrefix,
            dstPrefix = dstPrefix
          ).right.value

          result shouldBe a[PrefixTransferSuccess]
          result.asInstanceOf[PrefixTransferSuccess].successes should contain theSameElementsAs expectedResults

          expectedResults.zip(valuesT).map { case (result, t) =>
            store.get(result.source).right.value shouldBe Identified(result.source, t)
            store.get(result.destination).right.value shouldBe Identified(result.destination, t)
          }
        }
      }
    }
  }

  it("does not copy items from outside the prefix") {
    withNamespace { implicit namespace =>
      val srcPrefix = createPrefix
      val dstPrefix = createPrefix

      val srcLocations = (1 to 5).map { i => createLocationFrom(srcPrefix, suffix = s"$i.txt") }
      val dstLocations = (1 to 5).map { i => createLocationFrom(dstPrefix, suffix = s"$i.txt") }

      val valuesT = (1 to 5).map { _ => createT }

      val expectedResults: Seq[TransferPerformed[Location]] = srcLocations.zip(dstLocations).map { case (src, dst) =>
        TransferPerformed(src, dst)
      }

      val otherPrefix = createPrefix
      val otherLocation = createLocationFrom(otherPrefix, suffix = "other.txt")

      val initialEntries = srcLocations.zip(valuesT).toMap ++ Map(otherLocation -> createT)

      withPrefixTransferStore(initialEntries = initialEntries) { implicit store =>
        withPrefixTransfer { prefixTransfer =>
          val result = prefixTransfer.transferPrefix(
            srcPrefix = srcPrefix,
            dstPrefix = dstPrefix
          ).right.value

          result shouldBe a[PrefixTransferSuccess]
          result.asInstanceOf[PrefixTransferSuccess].successes should contain theSameElementsAs expectedResults

          expectedResults.zip(valuesT).map { case (result, t) =>
            store.get(result.source).right.value shouldBe Identified(result.source, t)
            store.get(result.destination).right.value shouldBe Identified(result.destination, t)
          }
        }
      }
    }
  }

  it("fails if the listing includes an item that doesn't exist") {
    withNamespace { implicit namespace =>
      val srcPrefix = createPrefix

      val srcLocations = (1 to 5).map { i => createLocationFrom(srcPrefix, suffix = s"$i.txt") }

      val valuesT = (1 to 5).map { _ => createT }

      withPrefixTransferStore(initialEntries = srcLocations.zip(valuesT).toMap) { implicit store =>
        withExtraListingTransfer { prefixTransfer =>
          val result = prefixTransfer.transferPrefix(
            srcPrefix = srcPrefix,
            dstPrefix = createPrefix
          ).left.value

          result shouldBe a[PrefixTransferFailure]
          val failure = result.asInstanceOf[PrefixTransferFailure]
          failure.successes.size shouldBe 5
          failure.failures.size shouldBe 1
        }
      }
    }
  }

  it("fails if the underlying transfer is broken") {
    withNamespace { implicit namespace =>
      val srcPrefix = createPrefix
      val srcLocation = createLocationFrom(srcPrefix, suffix = "1.txt")

      val t = createT

      withPrefixTransferStore(initialEntries = Map(srcLocation -> t)) { implicit store =>
        withBrokenTransfer { prefixTransfer =>
          val result = prefixTransfer.transferPrefix(
            srcPrefix = srcPrefix,
            dstPrefix = createPrefix
          ).left.value

          result shouldBe a[PrefixTransferFailure]
        }
      }
    }
  }

  it("fails if the underlying listing is broken") {
    withNamespace { implicit namespace =>
      withPrefixTransferStore(initialEntries = Map.empty) { implicit store =>
        withBrokenListingTransfer { prefixTransfer =>
          val result = prefixTransfer.transferPrefix(
            srcPrefix = createPrefix,
            dstPrefix = createPrefix
          ).left.value

          result shouldBe a[PrefixTransferListingFailure[_]]
        }
      }
    }
  }
}

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
