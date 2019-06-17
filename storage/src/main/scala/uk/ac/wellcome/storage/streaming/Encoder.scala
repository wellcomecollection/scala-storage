package uk.ac.wellcome.storage.streaming

import java.nio.charset.{Charset, StandardCharsets}

import grizzled.slf4j.Logging
import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.toJson
import uk.ac.wellcome.storage.{EncoderError, JsonEncodingError}

import scala.util.{Failure, Success}

trait Encoder[T] {
  type EncoderResult = Either[EncoderError, FiniteInputStream]

  def toStream(t: T): EncoderResult
}

object EncoderInstances extends Logging {
  implicit def stringEncoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Encoder[String] =
    (t: String) => {
      trace(s"Encoding string <$t> with charset <$charset>")

      // We tried to write a test that causes toInputStream to throw,
      // but were unable to find a way to do so! If you get an
      // exception from this, wrap it in a Try and add a test case (please).
      Right(
        new FiniteInputStream(
          IOUtils.toInputStream(t, charset),
          length = t.getBytes.length
        )
      )
    }

  // Circe uses the UTF-8 encoder internally
  implicit val jsonEncoder: Encoder[Json] =
    (t: Json) => stringEncoder.toStream(t.noSpaces)

  implicit def typeEncoder[T](implicit enc: circe.Encoder[T]): Encoder[T] =
    (t: T) =>
      toJson(t) match {
        case Success(jsonString) => stringEncoder.toStream(jsonString)
        case Failure(err)        => Left(JsonEncodingError(err))
    }

  implicit val streamEncoder: Encoder[FiniteInputStream] =
    (t: FiniteInputStream) => Right(t)
}
