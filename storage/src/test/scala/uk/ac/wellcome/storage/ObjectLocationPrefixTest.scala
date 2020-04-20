package uk.ac.wellcome.storage

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ObjectLocationPrefixTest extends AnyFunSpec with Matchers {
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
