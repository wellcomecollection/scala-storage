package uk.ac.wellcome.storage.type_classes

import org.scalatest.{FunSpec, Matchers}
import shapeless.syntax.singleton._
import shapeless.{the => sThe, _}
import uk.ac.wellcome.storage.ObjectLocation

class HybridRecordEnricherTest extends FunSpec with Matchers {

  case class Metadata(something: String)

  it("""generates a HList with all the fields from HybridRecord
      and extra fields from whatever type passed""") {
    val hybridRecordEnricher = sThe[HybridRecordEnricher[Metadata]]

    val metadata = Metadata("something")

    val id = "1111"
    val version = 3
    val location = ObjectLocation(namespace = "bukkit", key = "skeleton.txt")
    val hList =
      hybridRecordEnricher.enrichedHybridRecordHList(id, metadata, version)(
        location)

    val expectedHList =
      ("id" ->> id) ::
        ("version" ->> version) ::
        ("location" ->> location) ::
        ("something" ->> "something") :: HNil

    hList shouldBe expectedHList
  }
}
