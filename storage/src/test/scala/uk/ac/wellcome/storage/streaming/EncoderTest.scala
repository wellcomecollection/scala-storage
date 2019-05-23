package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.StandardCharsets

import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil.{toJson, _}
import uk.ac.wellcome.storage.JsonEncodingError

import scala.util.Random

class EncoderTest
  extends FunSpec
    with EitherValues
    with Matchers {

  import EncoderInstances._

  it("encodes a string") {
    val randomString = Random.nextString(8)
    val stream = stringEncoder.toStream(randomString)

    assertStreamEquals(stream.right.value, randomString)
  }

  it("encodes some json") {
    val randomString = Random.nextString(8)
    val randomJsonString = Json.fromString(randomString)

    val stream = jsonEncoder.toStream(randomJsonString)

    assertStreamEquals(stream.right.value, toJson(randomString).get)
  }

  it("encodes some type T") {
    case class FilmStar(name: String, age: Int)

    val michael = FilmStar("Michael J. Fox", 57)
    val stream = typeEncoder[FilmStar].toStream(michael)

    assertStreamEquals(stream.right.value, toJson(michael).get)
  }

  it("fails to encode if the circe encoder is broken") {
    case class FilmStar(name: String, age: Int)

    val brokenEncoder = new circe.Encoder[FilmStar] {
      override def apply(a: FilmStar): Json =
        throw new Throwable("boom")
    }

    val michael = FilmStar("Michael J. Fox", 57)
    val stream = typeEncoder[FilmStar](brokenEncoder).toStream(michael)

    stream.left.value shouldBe a[JsonEncodingError]
  }

  it("encodes a stream as itself") {
    val randomString = Random.nextString(8)
    val stream = stringEncoder.toStream(randomString).right.value

    streamEncoder.toStream(stream).right.value shouldBe stream
  }

  private def assertStreamEquals(inputStream: InputStream, string: String) =
    IOUtils.contentEquals(
      inputStream,
      IOUtils.toInputStream(
        string,
        StandardCharsets.UTF_8
      )
    ) shouldBe true
}
