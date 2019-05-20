package uk.ac.wellcome.storage.vhs

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.memory.{MemoryConditionalUpdateDao, MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.{KeyPrefix, KeySuffix, ObjectLocation}

import scala.util.{Failure, Success, Try}

class VersionedHybridStoreTest extends FunSpec with Matchers {

  case class Shape(
    name: String,
    sides: Int
  )

  case class ShapeMetadata(
    colour: String
  )

  type ShapeStore = MemoryObjectStore[Shape]
  type ShapeDao = MemoryVersionedDao[String, ShapeEntry]
  type ShapeVHS = VersionedHybridStore[String, Shape, ShapeMetadata]
  type ShapeEntry = Entry[String, ShapeMetadata]

  def createStore: ShapeStore = new MemoryObjectStore[Shape]()
  def createDao: ShapeDao = MemoryVersionedDao[String, ShapeEntry]()
  def createVhs(
    store: ShapeStore = createStore,
    dao: ShapeDao = createDao,
    testNamespace: String = "testing"
  ): ShapeVHS =
    new VersionedHybridStore[String, Shape, ShapeMetadata] {
      override protected val versionedDao: ShapeDao = dao
      override protected val objectStore: ShapeStore = store
      override protected val namespace: String = testNamespace
    }

  val triangle = Shape(name = "triangle", sides = 3)
  val rectangle = Shape(name = "rectangle", sides = 4)

  val metadataRed = ShapeMetadata(colour = "red")
  val metadataBlue = ShapeMetadata(colour = "blue")

  def storeNew(vhs: ShapeVHS, id: String = "shape1", shape: Shape = triangle, metadata: ShapeMetadata = metadataRed): Try[ShapeEntry] =
    vhs.update(id = id)(ifNotExisting = (shape, metadata))(ifExisting = (t, m) => (t, m))

  def storeUpdate(vhs: ShapeVHS, id: String = "shape1", newShape: Shape = rectangle, newMetadata: ShapeMetadata = metadataBlue): Try[ShapeEntry] =
    vhs.update(id = id)(ifNotExisting = (triangle, metadataRed))(ifExisting = (t, m) => (newShape, newMetadata))

  it("is consistent with itself") {
    val vhs = createVhs()

    vhs.get(id = "shape1") shouldBe Success(None)

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
      val store = createStore
      val vhs = createVhs(store = store)

      val result = storeNew(vhs, shape = triangle)
      result shouldBe a[Success[_]]

      val location = result.get.location
      store.get(location) shouldBe Success(triangle)
    }

    it("stores the metadata in the dao") {
      val dao = createDao
      val vhs = createVhs(dao = dao)

      val result = storeNew(vhs, metadata = metadataRed)
      result shouldBe a[Success[_]]

      val expectedRecord: ShapeEntry = result.get
      val storedRecord: ShapeEntry = dao.get(id = "shape1").get.get

      expectedRecord shouldBe storedRecord
    }

    it("stores an object with the id as a prefix") {
      val vhs = createVhs()

      val result = storeNew(vhs, id = "myGreatShapes")

      val location = result.get.location
      location.key should startWith("myGreatShapes/")
    }

    it("stores the object in the specified namespace") {
      val vhs = createVhs(testNamespace = "shapesorter")

      val result = storeNew(vhs)

      val location = result.get.location
      location.namespace shouldBe "shapesorter"
    }
  }

  describe("updating an existing record") {
    it("updates the object in the store") {
      val store = createStore
      val vhs = createVhs(store = store)
      storeNew(vhs, shape = triangle, metadata = metadataRed)

      val result = storeUpdate(vhs, newShape = rectangle)
      result shouldBe a[Success[_]]

      val location = result.get.location
      store.get(location) shouldBe Success(rectangle)
    }

    it("updates the metadata in the dao") {
      val dao = createDao
      val vhs = createVhs(dao = dao)
      storeNew(vhs, shape = triangle, metadata = metadataRed)

      val result = storeUpdate(vhs, newMetadata = metadataBlue)
      result shouldBe a[Success[_]]

      val expectedRecord: ShapeEntry = result.get
      val storedRecord: ShapeEntry = dao.get(id = "shape1").get.get

      expectedRecord shouldBe storedRecord
    }

    it("updates if only the object has changed") {
      val vhs = createVhs()

      val result1 = storeNew(vhs, shape = triangle, metadata = metadataRed)
      val result2 = storeUpdate(vhs, newShape = rectangle, newMetadata = metadataRed)

      result1 should not be result2
    }

    it("updates if only the metadata has changed") {
      val vhs = createVhs()

      val result1 = storeNew(vhs, shape = triangle, metadata = metadataRed)
      val result2 = storeUpdate(vhs, newShape = triangle, newMetadata = metadataBlue)

      result1 should not be result2
    }

    it("skips the update if nothing has changed") {
      val vhs = createVhs()

      val result1 = storeNew(vhs, shape = triangle, metadata = metadataRed)
      val result2 = storeUpdate(vhs, newShape = triangle, newMetadata = metadataRed)

      result1 shouldBe result2
    }
  }

  describe("errors when storing the record") {
    it("fails if the object store has an error") {
      val brokenStore: ShapeStore = new MemoryObjectStore[Shape]() {
        override def put(namespace: String)(
          input: Shape,
          keyPrefix: KeyPrefix = KeyPrefix(""),
          keySuffix: KeySuffix = KeySuffix(""),
          userMetadata: Map[String, String] = Map.empty
        ): Try[ObjectLocation] = Failure(new Throwable("BOOM!"))
      }

      val vhs = createVhs(store = brokenStore)

      val result = storeNew(vhs)
      result shouldBe a[Failure[_]]

      val err = result.failed.get
      err shouldBe a[Throwable]
      err.getMessage shouldBe "BOOM!"
    }

    it("fails if the dao has an error") {
      val brokenDao: ShapeDao = new MemoryVersionedDao[String, ShapeEntry](
        MemoryConditionalUpdateDao[String, ShapeEntry]()
      ) {
        override def put(value: ShapeEntry): Try[ShapeEntry] = Failure(new Throwable("BOOM!"))
      }

      val vhs = createVhs(dao = brokenDao)

      val result = storeNew(vhs)
      result shouldBe a[Failure[_]]

      val err = result.failed.get
      err shouldBe a[Throwable]
      err.getMessage shouldBe "BOOM!"
    }
  }

  describe("getting a record") {
    it("finds an existing object") {
      val vhs = createVhs()

      storeNew(vhs, id = "myShape", shape = triangle)

      vhs.get(id = "myShape") shouldBe Success(Some(triangle))
    }

    it("returns None if the id refers to a non-existent object") {
      val vhs = createVhs()

      vhs.get(id = "nonExistent") shouldBe Success(None)
    }
  }

  describe("errors when getting the record") {
    it("fails if the dao refers to a missing object in the store") {
      val forgetfulStore  = new MemoryObjectStore[Shape]() {
        override def put(namespace: String)(
          input: Shape,
          keyPrefix: KeyPrefix = KeyPrefix(""),
          keySuffix: KeySuffix = KeySuffix(""),
          userMetadata: Map[String, String] = Map.empty
        ): Try[ObjectLocation] = Success(
          ObjectLocation(namespace = namespace, key = "doesnotexist")
        )
      }

      val vhs = createVhs(store = forgetfulStore)

      storeNew(vhs, id = "myShape") shouldBe a[Success[_]]

      val result = vhs.get(id = "myShape")
      result shouldBe a[Failure[_]]

      val err = result.failed.get
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Dao entry for myShape points to a location that can't be fetched from the object store")
    }

    it("fails if the object store has an error") {
      val privateStore: ShapeStore = new MemoryObjectStore[Shape]() {
        override def get(objectLocation: ObjectLocation): Try[Shape] =
          Failure(new Throwable("Go away, nothing here!"))
      }

      val vhs = createVhs(store = privateStore)

      storeNew(vhs, id = "myShape")

      val result = vhs.get(id = "myShape")
      result shouldBe a[Failure[_]]

      val err = result.failed.get
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Dao entry for myShape points to a location that can't be fetched from the object store")
    }

    it("fails if the dao has an error") {
      val privateDao: ShapeDao = new MemoryVersionedDao[String, ShapeEntry](
        MemoryConditionalUpdateDao[String, ShapeEntry]()
      ) {
        override def get(id: String): Try[Option[ShapeEntry]] = Failure(new Throwable("Go away, nothing here!"))
      }

      val vhs = createVhs(dao = privateDao)

      val result = vhs.get(id = "myShape")
      result shouldBe a[Failure[_]]

      val err = result.failed.get
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Cannot read record myShape from dao")
    }
  }
}
