package uk.ac.wellcome.storage.streaming

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.{Charset, StandardCharsets}

import grizzled.slf4j.Logging
import io.circe
import io.circe.Json
import uk.ac.wellcome.json.JsonUtil.toJson
import uk.ac.wellcome.storage.{EncoderError, JsonEncodingError}

import scala.util.{Failure, Success}

trait Encoder[T] {
  type EncoderResult = Either[EncoderError, InputStream with FiniteStream]

  def toStream(t: T): EncoderResult
}

object EncoderInstances extends Logging {
  implicit val bytesEncoder: Encoder[Array[Byte]] =
    (bytes: Array[Byte]) =>
      Right(
        new FiniteInputStream(
          new ByteArrayInputStream(bytes),
          length = bytes.length
        )
    )

  implicit def stringEncoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Encoder[String] =
    (s: String) => {
      trace(s"Encoding string <$s> with charset <$charset>")

      bytesEncoder.toStream(s.getBytes(charset))
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

  implicit val streamEncoder: Encoder[InputStream with FiniteStream] =
    (t: InputStream with FiniteStream) => Right(t)
}
