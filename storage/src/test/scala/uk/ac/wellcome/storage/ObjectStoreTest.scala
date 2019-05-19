package uk.ac.wellcome.storage

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.memory.MemoryObjectStore

import scala.util.Success

class ObjectStoreTest extends FunSpec with Matchers with MockitoSugar with PropertyChecks with S3 {

  case class ObjectRecord(data: String)

  val namespace = "BetterObjectStoreTest_namespace"

  def createObjectStore[T](implicit strategy: SerialisationStrategy[T]): ObjectStore[T] =
    new MemoryObjectStore[T]()

  it("stores a record and can retrieve it") {
    val objectStore = createObjectStore[ObjectRecord]

    val record = ObjectRecord(data = "1")
    val location = objectStore.put(namespace)(record).get

    objectStore.get(location).get shouldBe record
  }

  it("writes the same object to the same key") {
    forAll { data: String =>
      val objectStore = createObjectStore[String]

      val location1 = objectStore.put(namespace)(data).get
      val location2 = objectStore.put(namespace)(data).get

      location1 shouldBe location2
    }
  }

  it("writes different objects to different keys") {
    forAll { (str1: String, str2: String) =>
      whenever(str1 != str2) {
        val objectStore = createObjectStore[String]

        val location1 = objectStore.put(namespace)(str1).get
        val location2 = objectStore.put(namespace)(str2).get

        (location1 == location2) shouldBe false
      }
    }
  }

  // This test is based on a specific failure we saw in the catalogue
  // pipeline, when a weak hashing algorithm led to unwanted collisions!
  it("writes different objects to different keys (specific example)") {
    val objectStore = createObjectStore[String]

    val str1 = """{"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"L0011975"},"otherIdentifiers":[{"identifierType":{"id":"sierra-system-number","label":"Sierra system number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"b12917175"}],"mergeCandidates":[],"title":"Antonio Dionisi","workType":null,"description":null,"physicalDescription":null,"extent":null,"lettering":null,"createdDate":null,"subjects":[],"genres":[{"label":"Book","concepts":[{"agent":{"label":"Book","ontologyType":"Concept","type":"Concept"},"type":"Unidentifiable"}],"ontologyType":"Genre"}],"contributors":[],"thumbnail":{"url":"https://iiif.wellcomecollection.org/image/L0011975.jpg/full/300,/0/default.jpg","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"thumbnail-image","label":"Thumbnail Image","ontologyType":"LocationType"},"credit":null,"ontologyType":"DigitalLocation","type":"DigitalLocation"},"production":[],"language":null,"dimensions":null,"items":[{"agent":{"locations":[{"url":"https://iiif.wellcomecollection.org/image/L0011975.jpg/info.json","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"iiif-image","label":"IIIF image","ontologyType":"LocationType"},"credit":"Wellcome Collection","ontologyType":"DigitalLocation","type":"DigitalLocation"}],"ontologyType":"Item"},"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Item","value":"L0011975"},"otherIdentifiers":[],"identifiedType":"Identified"}],"version":51,"ontologyType":"Work","identifiedType":"IdentifiedWork","type":"UnidentifiedWork"}"""
    val str2 = """{"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"L0023034"},"otherIdentifiers":[{"identifierType":{"id":"sierra-system-number","label":"Sierra system number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"b12074536"}],"mergeCandidates":[],"title":"Greenfield Sluder, Tonsillectomy..., use of guillotine","workType":null,"description":"Use of the guillotine","physicalDescription":null,"extent":null,"lettering":null,"createdDate":null,"subjects":[{"label":"Surgery","concepts":[{"agent":{"label":"Surgery","ontologyType":"Concept","type":"Concept"},"type":"Unidentifiable"}],"ontologyType":"Subject"}],"genres":[],"contributors":[],"thumbnail":{"url":"https://iiif.wellcomecollection.org/image/L0023034.jpg/full/300,/0/default.jpg","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"thumbnail-image","label":"Thumbnail Image","ontologyType":"LocationType"},"credit":null,"ontologyType":"DigitalLocation","type":"DigitalLocation"},"production":[],"language":null,"dimensions":null,"items":[{"agent":{"locations":[{"url":"https://iiif.wellcomecollection.org/image/L0023034.jpg/info.json","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"iiif-image","label":"IIIF image","ontologyType":"LocationType"},"credit":"Wellcome Collection","ontologyType":"DigitalLocation","type":"DigitalLocation"}],"ontologyType":"Item"},"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Item","value":"L0023034"},"otherIdentifiers":[],"identifiedType":"Identified"}],"version":50,"ontologyType":"Work","identifiedType":"IdentifiedWork","type":"UnidentifiedWork"}"""

    val location1 = objectStore.put(namespace)(str1).get
    val location2 = objectStore.put(namespace)(str2).get

    (location1 == location2) shouldBe false
  }

  it("closes the input stream when retrieving an object if the returned type is not input stream") {
    val mockBackend = mock[StorageBackend]

    val objectStore = new ObjectStore[String] {
      override implicit val serialisationStrategy: SerialisationStrategy[String] = SerialisationStrategy.stringStrategy
      override implicit val storageBackend: StorageBackend = mockBackend
    }

    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(mockBackend.get(location)).thenReturn(Success(stream))

    objectStore.get(location).get shouldBe string

    intercept[IOException] {
      stream.read()
    }
  }

  it("does not close the stream when retrieving an object if the returned type is InputStream") {
    val mockBackend = mock[StorageBackend]

    val objectStore = new ObjectStore[String] {
      override implicit val serialisationStrategy: SerialisationStrategy[String] = SerialisationStrategy.stringStrategy
      override implicit val storageBackend: StorageBackend = mockBackend
    }

    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(mockBackend.get(location)).thenReturn(Success(stream))

    objectStore.get(location).get shouldBe string

    stream.close()
  }

  it("uses the namespace, prefix and suffix, if supplied") {
    val objectStore = createObjectStore[String]

    val location = objectStore.put(namespace = "bucket")(
      input = "HelloWorld",
      keyPrefix = KeyPrefix("prefix"),
      keySuffix = KeySuffix(".suffix")
    ).get

    location.namespace shouldBe "bucket"
    location.key should startWith("prefix/")
    location.key should endWith(".suffix")
  }
}
