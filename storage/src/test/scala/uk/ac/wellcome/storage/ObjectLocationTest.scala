package uk.ac.wellcome.storage

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ObjectLocationTest extends AnyFunSpec with Matchers {
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

  it("normalizes the joined path") {
    val root = ObjectLocation(namespace = namespace, path = "images")
    val file =
      ObjectLocation(namespace = namespace, path = "images/red/dogs/001.jpg")

    root.join("./red", "dogs", "001.jpg") shouldBe file
  }
}
