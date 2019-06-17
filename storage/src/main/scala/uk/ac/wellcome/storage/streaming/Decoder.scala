package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.{Charset, StandardCharsets}

import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.fromJson
import uk.ac.wellcome.storage.{
  DecoderError,
  JsonDecodingError,
  StringDecodingError
}

import scala.util.{Failure, Success, Try}

trait Decoder[T] {
  type DecoderResult[S] = Either[DecoderError, S]

  def fromStream(inputStream: InputStream): DecoderResult[T]
}

object DecoderInstances {
  import io.circe.parser._

  type ParseJson[T] = String => Either[JsonDecodingError, T]

  implicit def stringDecoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Decoder[String] =
    (inputStream: InputStream) =>
      Try {
        IOUtils.toString(inputStream, charset)
      } match {
        case Success(string) => Right(string)
        case Failure(err)    => Left(StringDecodingError(err))
    }

  implicit val jsonDecoder: Decoder[Json] = (inputStream: InputStream) => {
    val parseJson: ParseJson[Json] = parse(_) match {
      case Left(err)   => Left(JsonDecodingError(err))
      case Right(json) => Right(json)
    }

    for {
      jsonString <- stringDecoder.fromStream(inputStream)
      result <- parseJson(jsonString)
    } yield result
  }

  implicit def typeDecoder[T](implicit dec: circe.Decoder[T]): Decoder[T] =
    (inputStream: InputStream) => {
      val parseJson: ParseJson[T] = fromJson[T](_) match {
        case Failure(err) => Left(JsonDecodingError(err))
        case Success(t)   => Right(t)
      }

      for {
        jsonString <- stringDecoder.fromStream(inputStream)
        result <- parseJson(jsonString)
      } yield result
    }

  implicit val streamDecoder: Decoder[InputStream] =
    (inputStream: InputStream) => Right(inputStream)
}
