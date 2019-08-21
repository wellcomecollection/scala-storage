package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}

class ObjectLocationTest extends FunSpec with Matchers {
  val namespace = "my_great_namespace"

  it("can join with a single path") {
    val root = ObjectLocation(namespace = namespace, path = "images/")
    val file = ObjectLocation(namespace = namespace, path = "images/001.jpg")

    root.join("001.jpg") shouldBe file
  }

  it("adds the trailing slash") {
    val root = ObjectLocation(namespace = namespace, path = "images")
    val file = ObjectLocation(namespace = namespace, path = "images/001.jpg")

    root.join("001.jpg") shouldBe file
  }

  it("can join multiple parts") {
    val root = ObjectLocation(namespace = namespace, path = "images")
    val file =
      ObjectLocation(namespace = namespace, path = "images/red/dogs/001.jpg")

    root.join("red", "dogs", "001.jpg") shouldBe file
  }
}
