package uk.ac.wellcome.storage

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.fixtures.MemoryBuilders
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.streaming.CodecInstances._

class ObjectStoreTest extends FunSpec with Matchers with MockitoSugar with PropertyChecks with MemoryBuilders with EitherValues with ObjectLocationGenerators {

  case class ObjectRecord(data: String)

  val namespace = "BetterObjectStoreTest_namespace"

  it("stores a record and can retrieve it") {
    val objectStore = createObjectStore[ObjectRecord]

    val record = ObjectRecord(data = "1")
    val location = objectStore.put(namespace)(record).right.value

    objectStore.get(location).right.value shouldBe record
  }

  it("writes objects to different keys") {
    forAll { (str1: String, str2: String) =>
      val objectStore = createObjectStore[String]

      val location1 = objectStore.put(namespace)(str1).right.value
      val location2 = objectStore.put(namespace)(str2).right.value

      (location1 == location2) shouldBe false
    }
  }

  it("if it's retrieving something that isn't a raw input stream, it closes the underlying input stream") {
    val mockBackend = mock[StorageBackend]

    val objectStore = new ObjectStore[String] {
      override implicit val codec: Codec[String] = stringCodec
      override implicit val storageBackend: StorageBackend = mockBackend
    }

    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(mockBackend.get(location)).thenReturn(Right(stream))

    objectStore.get(location).right.value shouldBe string

    intercept[IOException] {
      stream.read()
    }
  }

  it("if it's retrieving a raw input stream, it doesn't close it") {
    val mockBackend = mock[StorageBackend]

    val objectStore = new ObjectStore[String] {
      override implicit val codec: Codec[String] = stringCodec
      override implicit val storageBackend: StorageBackend = mockBackend
    }

    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(mockBackend.get(location)).thenReturn(Right(stream))

    objectStore.get(location).right.value shouldBe string

    stream.close()
  }

  it("uses the namespace, prefix and suffix, if supplied") {
    val objectStore = createObjectStore[String]

    val location = objectStore.put(namespace = "bucket")(
      input = "HelloWorld",
      keyPrefix = KeyPrefix("prefix"),
      keySuffix = KeySuffix(".suffix")
    ).right.value

    location.namespace shouldBe "bucket"
    location.key should startWith("prefix/")
    location.key should endWith(".suffix")
  }
}
