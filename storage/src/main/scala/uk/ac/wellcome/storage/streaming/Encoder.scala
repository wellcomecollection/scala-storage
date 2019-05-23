package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.{Charset, StandardCharsets}

import grizzled.slf4j.Logging
import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.toJson
import uk.ac.wellcome.storage.{EncoderError, JsonEncodingError}

import scala.util.{Failure, Success}

trait Encoder[T] {
  def toStream(t: T): Either[EncoderError, InputStream]
}

object EncoderInstances extends Logging {
  implicit def stringEncoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Encoder[String] =
    (t: String) =>  {
      info(s"Encoding string <$t> with charset <$charset>")

      // We tried to write a test that causes toInputStream to throw,
      // but were unable to find a way to do so! If you get an
      // exception from this, wrap it in a Try and add a test case (please).
      Right(IOUtils.toInputStream(t, charset))
    }

  // Circe uses the UTF-8 encoder internally
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