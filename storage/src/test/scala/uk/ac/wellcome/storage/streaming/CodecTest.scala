package uk.ac.wellcome.storage.streaming

import java.nio.charset.StandardCharsets

import io.circe.Json
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.LossyEncodingDetected
import uk.ac.wellcome.storage.generators.RandomThings

import scala.util.Random

class CodecTest
    extends FunSpec
    with Matchers
    with EitherValues
    with RandomThings {

  import Codec._

  describe("Codec") {
    describe("instances") {
      describe("it is consistent for") {
        it("a byte array") {
          val bytes = randomBytes()

          val stream = bytesCodec.toStream(bytes).right.value
          bytesCodec.fromStream(stream).right.value shouldBe bytes
        }

        it("a string") {
          val randomString = Random.nextString(8)

          val stream = stringCodec.toStream(randomString).right.value
          stringCodec.fromStream(stream).right.value shouldBe randomString
        }

        it("some json") {
          val randomString = Random.nextString(8)
          val randomJson = Json.fromString(randomString)

          val stream = jsonCodec.toStream(randomJson).right.value
          jsonCodec.fromStream(stream).right.value shouldBe randomJson
        }

        it("a type T") {
          case class NamedThing(name: String, value: Int)
          val thing = NamedThing(name = Random.nextString(8), value = 5)

          val stream = typeCodec[NamedThing].toStream(thing).right.value
          typeCodec[NamedThing].fromStream(stream).right.value shouldBe thing
        }
      }
    }

    describe("methods") {
      describe("coerce") {
        it("can coerce a valid string into a StandardCharset") {
          val randomString = randomLowercaseLatinAlphabetString()

          Codec
            .coerce(StandardCharsets.UTF_8)(randomString)
            .right
            .value shouldBe randomString
        }

        it("errors when coercing an invalid string into a StandardCharset") {
          Codec
            .coerce(StandardCharsets.US_ASCII)(randomUTF16String)
            .left
            .value shouldBe a[LossyEncodingDetected]
        }
      }
    }
  }
}
