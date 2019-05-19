package uk.ac.wellcome.storage

import java.io.InputStream

import io.circe.Json
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._

import scala.util.Success

class SerialisationStrategyTest extends FunSpec with Matchers {
  describe("string serialisation") {
    val stringStrategy: SerialisationStrategy[String] = SerialisationStrategy.stringStrategy

    it("converts a string to a StorageStream and back") {
      val result = stringStrategy.toStream(t = "Hello world")

      stringStrategy.fromStream(result.inputStream) shouldBe Success("Hello world")
    }

    it("uses the SHA256 hash of the string as the storage key") {
      val result = stringStrategy.toStream(t = "Hello world")

      // sha256("Hello world")
      result.storageKey.value shouldBe "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c"
    }
  }

  describe("JSON serialisation") {
    val jsonStrategy: SerialisationStrategy[Json] = SerialisationStrategy.jsonStrategy(
      stringStrategy = SerialisationStrategy.stringStrategy
    )

    val redSquare: Json = Json.obj(
      ("name", Json.fromString("square")),
      ("colour", Json.fromString("red")),
      ("sides", Json.fromInt(4))
    )

    it("converts a JSON object to a StorageStream and back") {
      val result = jsonStrategy.toStream(t = redSquare)

      jsonStrategy.fromStream(result.inputStream) shouldBe Success(redSquare)
    }
  }

  describe("case classes") {
    case class Shape(name: String, colour: String, sides: Int)

    val redSquare = Shape(
      name = "square",
      colour = "red",
      sides = 4
    )

    val shapeStrategy: SerialisationStrategy[Shape] = SerialisationStrategy.typeStrategy[Shape]

    it("converts a Shape to a StorageStream and back") {
      val result = shapeStrategy.toStream(t = redSquare)

      shapeStrategy.fromStream(result.inputStream) shouldBe Success(redSquare)
    }
  }

  describe("input streams") {
    val stringStrategy: SerialisationStrategy[String] = SerialisationStrategy.stringStrategy
    val streamStrategy: SerialisationStrategy[InputStream] = SerialisationStrategy.streamStrategy

    it("converts an InputStream to a StorageStream and back") {
      val inputStream = stringStrategy
        .toStream("Hello world")
        .inputStream

      val result = streamStrategy.toStream(t = inputStream)

      val retrieved = streamStrategy.fromStream(result.inputStream)
      retrieved shouldBe a[Success[_]]
      stringStrategy.fromStream(retrieved.get) shouldBe Success("Hello world")
    }
  }
}
