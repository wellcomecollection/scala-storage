package uk.ac.wellcome.storage.streaming

import io.circe
import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil.{toJson, _}
import uk.ac.wellcome.storage.JsonEncodingError

import scala.util.Random

class EncoderTest
  extends FunSpec
    with EitherValues
    with Matchers
    with StreamAssertions {

  import EncoderInstances._

  describe("successfully encodes") {
    it("a string") {
      val randomString = Random.nextString(8)
      val stream = stringEncoder.toStream(randomString)

      assertStreamEquals(stream.right.value, randomString, expectedLength = randomString.getBytes.length)
    }

    it("some json") {
      val randomString = Random.nextString(8)
      val randomJson = Json.fromString(randomString)

      val stream = jsonEncoder.toStream(randomJson)

      // len( "{8 chars}" ) ~> 10
      assertStreamEquals(stream.right.value, toJson(randomString).get, expectedLength = randomJson.noSpaces.getBytes.length)
    }

    it("a type T") {
      case class FilmStar(name: String, age: Int)

      val michael = FilmStar("Michael J. Fox", 57)
      val stream = typeEncoder[FilmStar].toStream(michael)

      // len( {"name":"Michael J. Fox","age":14"} ) ~> 34
      assertStreamEquals(stream.right.value, toJson(michael).get, expectedLength = 34)
    }

    it("a stream as itself") {
      val randomString = Random.nextString(8)
      val stream = stringEncoder.toStream(randomString).right.value

      streamEncoder.toStream(stream).right.value shouldBe stream
    }
  }

  it("fails to encode if the Circe encoder is broken") {
    case class FilmStar(name: String, age: Int)

    val brokenEncoder = new circe.Encoder[FilmStar] {
      override def apply(a: FilmStar): Json =
        throw new Throwable("boom")
    }

    val michael = FilmStar("Michael J. Fox", 57)
    val stream = typeEncoder[FilmStar](brokenEncoder).toStream(michael)

    stream.left.value shouldBe a[JsonEncodingError]
  }
}
