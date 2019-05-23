package uk.ac.wellcome.storage.streaming

import java.nio.charset.StandardCharsets

import io.circe.parser.parse
import org.apache.commons.io.IOUtils
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.{JsonDecodingError, StringDecodingError}

import scala.util.Random

class DecoderTest extends FunSpec
  with EitherValues
  with Matchers {

  import DecoderInstances._

  it("decodes a string") {
    val randomString = Random.nextString(8)
    val randomStream = IOUtils.toInputStream(randomString, StandardCharsets.UTF_8)

    stringDecoder.fromStream(randomStream) shouldBe Right(randomString)
  }

  it("fails to decode a string from an invalid stream") {
    stringDecoder.fromStream(null).left.value shouldBe a[StringDecodingError]
  }

  it("decodes json") {
    val jsonString =
      s"""
         |{
         |  "title": "Back to the Future III",
         |  "year": 1990
         |}
       """.stripMargin

    val jsonStream = IOUtils.toInputStream(jsonString, StandardCharsets.UTF_8)

    jsonDecoder.fromStream(jsonStream) shouldBe parse(jsonString)
  }

  it("fails to decode an invalid json string") {
    val invalidJsonString = "Great Scott!"

    val jsonStream = IOUtils.toInputStream(invalidJsonString, StandardCharsets.UTF_8)

    jsonDecoder.fromStream(jsonStream).left.value shouldBe a[JsonDecodingError]
  }

  case class Vehicle(model: String, speed: Int)

  it("decodes a type T") {
    val vehicle = Vehicle("DeLorean", 88)

    val jsonString =
      s"""
         |{
         |  "model": "DeLorean",
         |  "speed": 88
         |}
       """.stripMargin

    val jsonStream = IOUtils.toInputStream(jsonString, StandardCharsets.UTF_8)

    typeDecoder[Vehicle].fromStream(jsonStream) shouldBe Right(vehicle)
  }

  it("fails to decode a type T from invalid JSON") {
    val invalidJsonString = "Great Scott!"

    val jsonStream = IOUtils.toInputStream(invalidJsonString, StandardCharsets.UTF_8)

    typeDecoder[Vehicle].fromStream(jsonStream).left.value shouldBe a[JsonDecodingError]
  }

  it("decodes a stream as itself") {
    val randomString = Random.nextString(8)
    val randomStream = IOUtils.toInputStream(randomString, StandardCharsets.UTF_8)

    streamDecoder.fromStream(randomStream).right.value shouldBe randomStream
  }
}