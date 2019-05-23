package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.Charset

import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.toJson
import uk.ac.wellcome.storage.{EncoderError, JsonEncodingError, StringEncodingError}

import scala.util.{Failure, Success, Try}

trait Encoder[T] {
  def toStream(t: T): Either[EncoderError, InputStream]
}

// test cases:
// - string success
// - string fails on bad charset
// - json succeeds
// - type succeeds
// - type fails on bad encoder
// - stream succeeds

object EncoderInstances {
  implicit val stringEncoder: Encoder[String] =
    (t: String) => Try {
      IOUtils.toInputStream(t, Charset.defaultCharset)
    } match {
      case Success(inputStream) => Right(inputStream)
      case Failure(err) => Left(StringEncodingError(err))
    }

  implicit val jsonEncoder: Encoder[Json] =
    (t: Json) => stringEncoder.toStream(t.noSpaces)

  implicit def typeEncoder[T](implicit enc: circe.Encoder[T]): Encoder[T] =
    (t: T) => toJson(t) match {
      case Success(jsonString) => stringEncoder.toStream(jsonString)
      case Failure(err) => Left(JsonEncodingError(err))
    }

  implicit val streamEncoder: Encoder[InputStream] =
    (t: InputStream) => Right(t)
}
