package uk.ac.wellcome.storage.vhs

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.MemoryBuilders
import uk.ac.wellcome.storage.memory.{MemoryConditionalUpdateDao, MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.streaming.CodecInstances._

class VersionedHybridStoreTest extends FunSpec with Matchers with EitherValues with MemoryBuilders {

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

  def createShapeVhs(
    store: ShapeStore = createObjectStore[Shape],
    dao: ShapeDao = createVersionedDao[ShapeEntry],
    testNamespace: String = "testing"
  ): ShapeVHS =
    createVhs[Shape, ShapeMetadata](
      store = createObjectStore[Shape],
      dao = createVersionedDao[ShapeEntry],
      testNamespace = testNamespace
    )

  val triangle = Shape(name = "triangle", sides = 3)
  val rectangle = Shape(name = "rectangle", sides = 4)

  val metadataRed = ShapeMetadata(colour = "red")
  val metadataBlue = ShapeMetadata(colour = "blue")

  def storeNew(vhs: ShapeVHS, id: String = "shape1", shape: Shape = triangle, metadata: ShapeMetadata = metadataRed): Either[StorageError, vhs.VHSEntry] =
    vhs.update(id = id)(ifNotExisting = (shape, metadata))(ifExisting = (t, m) => (t, m))

  def storeUpdate(vhs: ShapeVHS, id: String = "shape1", newShape: Shape = rectangle, newMetadata: ShapeMetadata = metadataBlue): Either[StorageError, vhs.VHSEntry] =
    vhs.update(id = id)(ifNotExisting = (triangle, metadataRed))(ifExisting = (t, m) => (newShape, newMetadata))

  it("is consistent with itself") {
    val vhs = createShapeVhs()

    vhs.get(id = "shape1").left.value shouldBe a[DoesNotExistError]

    vhs.update(id = "shape1")(ifNotExisting = (triangle, metadataRed))(ifExisting = (t, m) => (t, m)) shouldBe a[Right[_, _]]

    vhs.get(id = "shape1") shouldBe Right(triangle)

    val expected = Shape(name = "TRIANGLE", sides = 3)
    vhs.update(id = "shape1")(ifNotExisting = (triangle, metadataRed))(
      ifExisting = (t, m) => (t.copy(name = t.name.toUpperCase()), m)
    ) shouldBe Right(())

    vhs.get(id = "shape1") shouldBe Right(expected)
  }

  describe("storing a new record") {
    it("stores the object in the store") {
      val store = createObjectStore[Shape]
      val vhs = createShapeVhs(store = store)

      val result = storeNew(vhs, shape = triangle)
      result shouldBe a[Right[_, _]]

      val location = result.right.value.location
      store.get(location) shouldBe Right(triangle)
    }

    it("stores the metadata in the dao") {
      val dao = createVersionedDao[ShapeEntry]
      val vhs = createShapeVhs(dao = dao)

      val result = storeNew(vhs, metadata = metadataRed)
      result shouldBe a[Right[_, _]]

      val expectedRecord: ShapeEntry = result.right.value
      val storedRecord: ShapeEntry = dao.get(id = "shape1").right.value

      expectedRecord shouldBe storedRecord
    }

    it("stores an object with the id as a prefix") {
      val vhs = createShapeVhs()

      val result = storeNew(vhs, id = "myGreatShapes")

      val location = result.right.value.location
      location.key should startWith("myGreatShapes/")
    }

    it("stores the object in the specified namespace") {
      val vhs = createShapeVhs(testNamespace = "shapesorter")

      val result = storeNew(vhs)

      val location = result.right.value.location
      location.namespace shouldBe "shapesorter"
    }
  }

  describe("updating an existing record") {
    it("updates the object in the store") {
      val store = createObjectStore[Shape]
      val vhs = createShapeVhs(store = store)
      storeNew(vhs, shape = triangle, metadata = metadataRed)

      val result = storeUpdate(vhs, newShape = rectangle)
      result shouldBe a[Right[_, _]]

      val location = result.right.value.location
      store.get(location) shouldBe Right(rectangle)
    }

    it("updates the metadata in the dao") {
      val dao = createVersionedDao[ShapeEntry]
      val vhs = createShapeVhs(dao = dao)
      storeNew(vhs, shape = triangle, metadata = metadataRed)

      val storedRecord = storeUpdate(vhs, newMetadata = metadataBlue)
      storedRecord.right.value shouldBe a[Shape]

      dao.get(id = "shape1") shouldBe storedRecord
    }

    it("updates if only the object has changed") {
      val vhs = createShapeVhs()

      val result1 = storeNew(vhs, shape = triangle, metadata = metadataRed)
      val result2 = storeUpdate(vhs, newShape = rectangle, newMetadata = metadataRed)

      result1 should not be result2
    }

    it("updates if only the metadata has changed") {
      val vhs = createShapeVhs()

      val result1 = storeNew(vhs, shape = triangle, metadata = metadataRed)
      val result2 = storeUpdate(vhs, newShape = triangle, newMetadata = metadataBlue)

      result1 should not be result2
    }

    it("skips the update if nothing has changed") {
      val vhs = createShapeVhs()

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
        ): Either[WriteError, ObjectLocation] = Left(BackendWriteError(new Throwable("BOOM!")))
      }

      val vhs = createShapeVhs(store = brokenStore)

      val result = storeNew(vhs)

      val err = result.left.value.e
      err shouldBe a[Throwable]
      err.getMessage shouldBe "BOOM!"
    }

    it("fails if the dao has an error") {
      val brokenDao: ShapeDao = new MemoryVersionedDao[String, ShapeEntry](
        MemoryConditionalUpdateDao[String, ShapeEntry]()
      ) {
        override def put(value: ShapeEntry): Either[WriteError, ShapeEntry] = Left(DaoWriteError(new Throwable("BOOM!")))
      }

      val vhs = createShapeVhs(dao = brokenDao)

      val result = storeNew(vhs)

      val err = result.left.value.e
      err shouldBe a[Throwable]
      err.getMessage shouldBe "BOOM!"
    }
  }

  describe("getting a record") {
    it("finds an existing object") {
      val vhs = createShapeVhs()

      storeNew(vhs, id = "myShape", shape = triangle)

      vhs.get(id = "myShape") shouldBe Right(triangle)
    }

    it("returns None if the id refers to a non-existent object") {
      val vhs = createShapeVhs()

      vhs.get(id = "nonExistent").left.value shouldBe a[DoesNotExistError]
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
        ): Either[WriteError, ObjectLocation] = Right(
          ObjectLocation(namespace = namespace, key = "doesnotexist")
        )
      }

      val vhs = createShapeVhs(store = forgetfulStore)

      storeNew(vhs, id = "myShape") shouldBe a[Right[_, _]]

      val result = vhs.get(id = "myShape")

      val err = result.left.value.e
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Dao entry for myShape points to a location that can't be fetched from the object store")
    }

    it("fails if the object store has an error") {
      val privateStore: ShapeStore = new MemoryObjectStore[Shape]() {
        override def get(objectLocation: ObjectLocation): Either[ReadError, Shape] =
          Left(BackendReadError(new Throwable("Go away, nothing here!")))
      }

      val vhs = createShapeVhs(store = privateStore)

      storeNew(vhs, id = "myShape")

      val result = vhs.get(id = "myShape")

      val err = result.left.value.e
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Dao entry for myShape points to a location that can't be fetched from the object store")
    }

    it("fails if the dao has an error") {
      val privateDao: ShapeDao = new MemoryVersionedDao[String, ShapeEntry](
        MemoryConditionalUpdateDao[String, ShapeEntry]()
      ) {
        override def get(id: String): Either[ReadError, ShapeEntry] = Left(DaoReadError(new Throwable("Go away, nothing here!")))
      }

      val vhs = createShapeVhs(dao = privateDao)

      val result = vhs.get(id = "myShape")

      val err = result.left.value.e
      err shouldBe a[Throwable]
      err.getMessage should startWith("Cannot read record myShape from dao")
    }
  }
}
