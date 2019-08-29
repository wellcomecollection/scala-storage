package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}

class ObjectLocationPrefixTest extends FunSpec with Matchers {
  val namespace = "my_great_namespace"

  it("normalizes the location") {
    val prefix = ObjectLocationPrefix(
      namespace = namespace,
      path = "images"
    )

    prefix.asLocation("./red/blood.jpg") shouldBe ObjectLocation(
      namespace = namespace,
      path = "images/red/blood.jpg"
    )
  }
}
