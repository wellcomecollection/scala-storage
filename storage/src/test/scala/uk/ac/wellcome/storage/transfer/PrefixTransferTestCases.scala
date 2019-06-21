//package uk.ac.wellcome.storage.transfer
//
//import org.scalatest.{EitherValues, FunSpec, Matchers}
//import uk.ac.wellcome.fixtures.TestWith
//import uk.ac.wellcome.storage.generators.RandomThings
//import uk.ac.wellcome.storage.listing.Listing
//import uk.ac.wellcome.storage.listing.fixtures.ListingFixtures
//import uk.ac.wellcome.storage.listing.memory.{MemoryListing, MemoryListingFixtures}
//import uk.ac.wellcome.storage.store.Store
//import uk.ac.wellcome.storage.store.memory.MemoryStore
//import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures
//import uk.ac.wellcome.storage.transfer.memory.{MemoryPrefixTransfer, MemoryTransfer, MemoryTransferFixtures}
//
//trait PrefixTransferFixtures[Location, Prefix, T, StoreImpl <: Store[Location, T], ListingImpl <: Listing[Prefix, Location], TransferImpl <: Transfer[Location], StoreContext]
//  extends TransferFixtures[Location, T, StoreImpl, TransferImpl, StoreContext]
//    with ListingFixtures[Location, Prefix, Location, ListingImpl, StoreContext] {
//  def withPrefixTransferContext[R](testWith: TestWith[StoreContext, R]): R =
//    withTransferStoreContext { context =>
//      testWith(context)
//    }
//
//  override def withListingContext[R](testWith: TestWith[StoreContext, R]): R =
//    withPrefixTransferContext { context =>
//      testWith(context)
//    }
//
//  def withPrefixTransfer[R](initialEntries: Map[Location, T])(testWith: TestWith[PrefixTransfer[Prefix, Location], R]): R
//}
//
//trait PrefixTransferTestCases[Location, Prefix, T, StoreImpl <: Store[Location, T], TransferImpl <: Transfer[Location], ListingImpl <: Listing[Prefix, Location], StoreContext] extends FunSpec with Matchers with PrefixTransferFixtures[Location, Prefix, T, StoreImpl, ListingImpl, TransferImpl, StoreContext] with EitherValues {
//  def createPrefix(implicit context: StoreContext): Prefix
//
//  def createLocationFrom(prefix: Prefix, suffix: String): Location
//
//  it("does nothing if the prefix is empty") {
//    withPrefixTransferContext { implicit context =>
//      withPrefixTransfer(initialEntries = Map.empty) { prefixTransfer =>
//        prefixTransfer.transfer(
//          srcPrefix = createPrefix,
//          dstPrefix = createPrefix
//        ).right.value shouldBe PrefixTransferSuccess(Seq.empty)
//      }
//    }
//  }
//
//  it("copies a single item") {
//    withPrefixTransferContext { implicit context =>
//      val srcPrefix = createPrefix
//      val dstPrefix = createPrefix
//
//      val srcLocation = createLocationFrom(srcPrefix, suffix = "1.txt")
//      val dstLocation = createLocationFrom(dstPrefix, suffix = "1.txt")
//
//      val t = createT
//
//      withPrefixTransfer(initialEntries = Map(srcLocation -> t)) { prefixTransfer =>
//        prefixTransfer.transfer(
//          srcPrefix = createPrefix,
//          dstPrefix = createPrefix
//        ).right.value shouldBe PrefixTransferSuccess(
//          Seq(TransferPerformed(srcLocation, dstLocation))
//        )
//      }
//    }
//  }
//
//  it("copies multiple items") {
//    true shouldBe false
//  }
//
//  it("fails if a single item fails to copy") {
//    true shouldBe false
//  }
//
//  it("fails if the underlying transfer is broken") {
//    true shouldBe false
//  }
//
//  it("fails if the underlying listing is broken") {
//    true shouldBe false
//  }
//}
//
//class MemoryPrefixTransferTest extends
//  PrefixTransferTestCases[String, String, Array[Byte], MemoryStore[String, Array[Byte]], MemoryTransfer[String, Array[Byte]], MemoryListing[String, String, Array[Byte]], MemoryStore[String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]]
//with MemoryTransferFixtures[String, Array[Byte]] with RandomThings {
//  override def createT: Array[Byte] = randomBytes()
//
//  def createPrefix(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric
//
//  override def createLocationFrom(prefix: String, suffix: String): String = prefix + "/" + suffix
//
//  override def withPrefixTransfer[R](initialEntries: Map[String, Array[Byte]])(testWith: TestWith[PrefixTransfer[String, String], R]): R = {
//    implicit val store: MemoryStore[String, Array[Byte]] = new MemoryStore[String, Array[Byte]](initialEntries)
//
//    withTransfer { memoryTransfer =>
//      withListing(store, initialEntries = Seq.empty) { memoryListing =>
//        val prefixTransfer = new MemoryPrefixTransfer[String, String, Array[Byte]] {
//          override implicit val transfer: MemoryTransfer[String, Array[Byte]] = memoryTransfer
//          override implicit val listing: MemoryListing[String, String, Array[Byte]] = memoryListing
//
//          override protected def buildDstLocation(srcPrefix: String, dstPrefix: String, srcLocation: String): String =
//            srcLocation.replaceAll("^" + srcPrefix, dstPrefix)
//        }
//
//        testWith(prefixTransfer)
//      }
//    }
//  }
//}
