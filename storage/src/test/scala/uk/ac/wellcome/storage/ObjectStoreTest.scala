package uk.ac.wellcome.storage

import java.io.{File, IOException}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.mockito.Mockito
import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.type_classes.SerialisationStrategy

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Success

class ObjectStoreTest extends FunSpec with S3 with ScalaFutures with PropertyChecks with MockitoSugar {
  case class TestClass(id: String)


  it("puts a record and is able to retrieve it") {
    withLocalS3Bucket { bucket =>
      val objectStore = ObjectStore[TestClass]

      val input = TestClass(id = "12345")
      val eventualLocation = objectStore.put(bucket.name)(input)

      whenReady(eventualLocation) { location =>
        whenReady(objectStore.get(location)) { output =>
          output shouldBe input
        }
      }
    }
  }

  it("writes the same object to the same key"){
    withLocalS3Bucket { bucket =>
      forAll { input: String =>
        val objectStore = ObjectStore[String]

        val eventualLocation1 = objectStore.put(bucket.name)(input)
        val eventualLocation2 = objectStore.put(bucket.name)(input)

        whenReady(Future.sequence(List(eventualLocation1, eventualLocation2))) { locations =>
          locations.distinct should have size 1
        }
      }
    }
  }

  it("writes different objects to different keys"){
    withLocalS3Bucket { bucket =>
      forAll { (str1: String, str2: String) =>
        whenever(str1 != str2) {
          val objectStore = ObjectStore[String]

          val eventualLocation1 = objectStore.put(bucket.name)(str1)
          val eventualLocation2 = objectStore.put(bucket.name)(str2)

          whenReady(Future.sequence(List(eventualLocation1, eventualLocation2))) { locations =>
            locations.distinct should have size 2
          }
        }
      }
    }
  }

  // This test is based on a specific failure we saw in the catalogue
  // pipeline, when a weak hashing algorithm led to unwanted collisions!
  it("writes different objects to different keys (specific example)") {
    withLocalS3Bucket { bucket =>
      val objectStore = ObjectStore[String]

      val str1 = """{"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"L0011975"},"otherIdentifiers":[{"identifierType":{"id":"sierra-system-number","label":"Sierra system number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"b12917175"}],"mergeCandidates":[],"title":"Antonio Dionisi","workType":null,"description":null,"physicalDescription":null,"extent":null,"lettering":null,"createdDate":null,"subjects":[],"genres":[{"label":"Book","concepts":[{"agent":{"label":"Book","ontologyType":"Concept","type":"Concept"},"type":"Unidentifiable"}],"ontologyType":"Genre"}],"contributors":[],"thumbnail":{"url":"https://iiif.wellcomecollection.org/image/L0011975.jpg/full/300,/0/default.jpg","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"thumbnail-image","label":"Thumbnail Image","ontologyType":"LocationType"},"credit":null,"ontologyType":"DigitalLocation","type":"DigitalLocation"},"production":[],"language":null,"dimensions":null,"items":[{"agent":{"locations":[{"url":"https://iiif.wellcomecollection.org/image/L0011975.jpg/info.json","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"iiif-image","label":"IIIF image","ontologyType":"LocationType"},"credit":"Wellcome Collection","ontologyType":"DigitalLocation","type":"DigitalLocation"}],"ontologyType":"Item"},"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Item","value":"L0011975"},"otherIdentifiers":[],"identifiedType":"Identified"}],"version":51,"ontologyType":"Work","identifiedType":"IdentifiedWork","type":"UnidentifiedWork"}"""
      val str2 = """{"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"L0023034"},"otherIdentifiers":[{"identifierType":{"id":"sierra-system-number","label":"Sierra system number","ontologyType":"IdentifierType"},"ontologyType":"Work","value":"b12074536"}],"mergeCandidates":[],"title":"Greenfield Sluder, Tonsillectomy..., use of guillotine","workType":null,"description":"Use of the guillotine","physicalDescription":null,"extent":null,"lettering":null,"createdDate":null,"subjects":[{"label":"Surgery","concepts":[{"agent":{"label":"Surgery","ontologyType":"Concept","type":"Concept"},"type":"Unidentifiable"}],"ontologyType":"Subject"}],"genres":[],"contributors":[],"thumbnail":{"url":"https://iiif.wellcomecollection.org/image/L0023034.jpg/full/300,/0/default.jpg","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"thumbnail-image","label":"Thumbnail Image","ontologyType":"LocationType"},"credit":null,"ontologyType":"DigitalLocation","type":"DigitalLocation"},"production":[],"language":null,"dimensions":null,"items":[{"agent":{"locations":[{"url":"https://iiif.wellcomecollection.org/image/L0023034.jpg/info.json","license":{"id":"cc-by","label":"Attribution 4.0 International (CC BY 4.0)","url":"http://creativecommons.org/licenses/by/4.0/","ontologyType":"License"},"locationType":{"id":"iiif-image","label":"IIIF image","ontologyType":"LocationType"},"credit":"Wellcome Collection","ontologyType":"DigitalLocation","type":"DigitalLocation"}],"ontologyType":"Item"},"sourceIdentifier":{"identifierType":{"id":"miro-image-number","label":"Miro image number","ontologyType":"IdentifierType"},"ontologyType":"Item","value":"L0023034"},"otherIdentifiers":[],"identifiedType":"Identified"}],"version":50,"ontologyType":"Work","identifiedType":"IdentifiedWork","type":"UnidentifiedWork"}"""

      val eventualLocation1 = objectStore.put(bucket.name)(str1)
      val eventualLocation2 = objectStore.put(bucket.name)(str2)

      whenReady(Future.sequence(List(eventualLocation1, eventualLocation2))) { locations =>
        locations.distinct should have size 2
      }
    }
  }

  it("closes the input stream when retrieving an object if the returned type is not input stream") {
    val storageBackend = mock[StorageBackend]

    val objectStore = ObjectStore.createObjectStore(
      storageStrategy = SerialisationStrategy.stringSerialisationStrategy,
      storageBackend = storageBackend,
      ec = implicitly[ExecutionContext]
    )
    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(storageBackend.get(location)).thenReturn(Success(stream))

    Await.result(objectStore.get(location), 10 seconds) shouldBe string

    intercept[IOException] {
      stream.read()
    }
  }

  it("does not close the stream when retrieving an object if the returned type is InputStream") {
    val storageBackend = mock[StorageBackend]

    val objectStore = ObjectStore.createObjectStore(
      storageStrategy = SerialisationStrategy.streamSerialisationStrategy,
      storageBackend = storageBackend,
      ec = implicitly[ExecutionContext]
    )
    val location = createObjectLocation

    val string = "bah buh bih"
    val file = File.createTempFile("stream-test", ".txt")
    FileUtils.writeStringToFile(file, string, "UTF-8")
    val stream = FileUtils.openInputStream(file)
    Mockito.when(storageBackend.get(location)).thenReturn(Success(stream))

    IOUtils.toString(Await.result(objectStore.get(location), 10 seconds), "UTF-8") shouldBe string

    stream.close()
  }
}
