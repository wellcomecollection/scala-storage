package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}

import scala.util.Success

class BetterVHSTest extends FunSpec with Matchers {

  case class Shape(
    name: String,
    sides: Int
  )

  case class ShapeMetadata(
    colour: String
  )

  type ShapeStore = MemoryObjectStore[Shape]
  type ShapeDao = MemoryVersionedDao[String, BetterVHSEntry[String, ShapeMetadata]]
  type ShapeVHS = BetterVHS[String, Shape, ShapeMetadata]

  def createStore: ShapeStore = new MemoryObjectStore[Shape]()
  def createDao: ShapeDao = MemoryVersionedDao[String, BetterVHSEntry[String, ShapeMetadata]]()
  def createVhs(
    store: ShapeStore = createStore,
    dao: ShapeDao = createDao,
    testNamespace: String = "testing"
  ): ShapeVHS =
    new BetterVHS[String, Shape, ShapeMetadata] {
      override protected val versionedDao: ShapeDao = dao
      override protected val objectStore: ShapeStore = store
      override protected val namespace: String = testNamespace
    }

  it("is consistent with itself") {
    val vhs = createVhs()

    vhs.get(id = "shape1") shouldBe Success(None)

    val triangle = Shape(name = "triangle", sides = 3)
    val metadataRed = ShapeMetadata(colour = "red")
    vhs.update(id = "shape1")(ifNotExisting = (triangle, metadataRed))(ifExisting = (t, m) => (t, m)) shouldBe a[Success[_]]

    vhs.get(id = "shape1") shouldBe Success(Some(triangle))

    val expected = Shape(name = "TRIANGLE", sides = 3)
    vhs.update(id = "shape1")(ifNotExisting = (triangle, metadataRed))(
      ifExisting = (t, m) => (t.copy(name = t.name.toUpperCase()), m)
    ) shouldBe a[Success[_]]

    vhs.get(id = "shape1") shouldBe Success(Some(expected))
  }

  describe("storing a new record") {
    it("stores the object in the store") {

    }

    it("stores the metadata in the dao") {}

    it("stores an object with the id as a prefix") {}

    it("stores the object in the specified namespace") {}
  }

  describe("updating an existing record") {
    it("stores the new object and metadata") {}

    it("updates if only the object has changed") {}

    it("updates if only the metadata has changed") {}

    it("skips the update if nothing has changed") {}
  }

  describe("errors when storing the record") {
    it("fails if the object store has an error") {}

    it("fails if the dao has an error") {}
  }

  describe("getting a record") {
    it("finds an existing object") {}

    it("returns None if the id refers to a non-existent object") {}
  }

  describe("errors when getting the record") {
    it("fails if the dao refers to a missing object in the store") {}

    it("fails if the object store has an error") {}

    it("fails if the dao has an error") {}
  }
}
