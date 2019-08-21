package uk.ac.wellcome.storage.listing.memory

import org.scalatest.Assertion
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.listing.ListingTestCases
import uk.ac.wellcome.storage.store.memory.MemoryStore

class MemoryListingTest
    extends ListingTestCases[
      String,
      String,
      String,
      MemoryListing[String, String, Array[Byte]],
      MemoryStore[String, Array[Byte]]]
    with MemoryListingFixtures[Array[Byte]]
    with RandomThings {
  def createT: Array[Byte] = randomBytes()

  override def createIdent(
    implicit context: MemoryStore[String, Array[Byte]]): String =
    randomAlphanumeric

  override def extendIdent(id: String, extension: String): String =
    id + extension

  override def createPrefix: String = randomAlphanumeric

  override def createPrefixMatching(id: String): String = id

  override def assertResultCorrect(result: Iterable[String],
                                   entries: Seq[String]): Assertion =
    result.toSeq should contain theSameElementsAs entries
}
