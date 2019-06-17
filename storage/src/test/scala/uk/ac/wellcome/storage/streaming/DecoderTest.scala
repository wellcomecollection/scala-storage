package uk.ac.wellcome.storage.streaming

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import io.circe
import io.circe.Decoder.Result
import io.circe.HCursor
import io.circe.parser.parse
import org.apache.commons.io.IOUtils
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage._

import scala.util.Random

class DecoderTest extends FunSpec
  with EitherValues
  with Matchers {

  import DecoderInstances._

  case class Vehicle(model: String, speed: Int)

  describe("successfully decodes") {
    it("a byte array") {
      val byteArray = Array[Byte](20)
      Random.nextBytes(byteArray)

      byteArray.length > 0 shouldBe true

      val stream = new FiniteInputStream(
        new ByteArrayInputStream(byteArray),
        length = byteArray.length
      )

      bytesDecoder.fromStream(stream).right.value shouldBe byteArray
    }

    it("a string") {
      val randomString = Random.nextString(8)
      val randomStream = createStream(randomString)

      stringDecoder.fromStream(randomStream) shouldBe Right(randomString)
    }

    it("json") {
      val jsonString =
        s"""
           |{
           |  "title": "Back to the Future III",
           |  "year": 1990
           |}
       """.stripMargin

      val jsonStream = createStream(jsonString)

      jsonDecoder.fromStream(jsonStream) shouldBe parse(jsonString)
    }

    it("a type T") {
      val vehicle = Vehicle("DeLorean", 88)

      val jsonString =
        s"""
           |{
           |  "model": "DeLorean",
           |  "speed": 88
           |}
       """.stripMargin

      val jsonStream = createStream(jsonString)

      typeDecoder[Vehicle].fromStream(jsonStream) shouldBe Right(vehicle)
    }

    it("a stream as itself") {
      val randomString = Random.nextString(8)
      val randomStream = createStream(randomString)

      streamDecoder.fromStream(randomStream).right.value shouldBe randomStream
    }
  }

  describe("fails to decode") {
    it("from an invalid stream") {
      stringDecoder.fromStream(null).left.value shouldBe a[StringDecodingError]
      jsonDecoder.fromStream(null).left.value shouldBe a[StringDecodingError]
      typeDecoder[Vehicle].fromStream(null).left.value shouldBe a[StringDecodingError]
    }

    it("an invalid json string") {
      val invalidJsonString = "Great Scott!"

      val jsonStream = createStream(invalidJsonString)

      jsonDecoder.fromStream(jsonStream).left.value shouldBe a[JsonDecodingError]
    }

    it("a type T from invalid JSON") {
      val invalidJsonString = "Great Scott!"

      val jsonStream = createStream(invalidJsonString)

      typeDecoder[Vehicle].fromStream(jsonStream).left.value shouldBe a[JsonDecodingError]
    }

    it("if the specified length is too long") {
      val badStream = new FiniteInputStream(
        IOUtils.toInputStream(Random.nextString(8), StandardCharsets.UTF_8),
        length = 100
      )

      stringDecoder.fromStream(badStream).left.value shouldBe a[IncorrectStreamLengthError]
    }

    it("if the specified length is too short") {
      val badStream = new FiniteInputStream(
        IOUtils.toInputStream(Random.nextString(8), StandardCharsets.UTF_8),
        length = 5
      )

      stringDecoder.fromStream(badStream).left.value shouldBe a[IncorrectStreamLengthError]
    }

    it("if the Circe decoder is broken") {
      val brokenDecoder = new circe.Decoder[Vehicle] {
        override def apply(c: HCursor): Result[Vehicle] =
          circe.Decoder[Int].apply(c).map { _ => Vehicle("delorean", 88) }
      }

      val jsonString =
        s"""
           |{
           |  "model": "DeLorean",
           |  "speed": 88
           |}
       """.stripMargin

      val stream = typeDecoder[Vehicle](brokenDecoder).fromStream(createStream(jsonString))

      stream.left.value shouldBe a[JsonDecodingError]
    }
  }

  def createStream(s: String): FiniteInputStream =
    new FiniteInputStream(
      IOUtils.toInputStream(s, StandardCharsets.UTF_8),
      length = s.getBytes.length
    )
}
