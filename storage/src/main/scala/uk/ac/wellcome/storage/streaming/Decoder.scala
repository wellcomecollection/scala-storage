package uk.ac.wellcome.storage.streaming

import java.nio.charset.{Charset, StandardCharsets}

import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.fromJson
import uk.ac.wellcome.storage.{
  DecoderError,
  IncorrectStreamLengthError,
  JsonDecodingError,
  StringDecodingError
}

import scala.util.{Failure, Success, Try}

trait Decoder[T] {
  type DecoderResult[S] = Either[DecoderError, S]

  def fromStream(inputStream: FiniteInputStream): DecoderResult[T]
}

object DecoderInstances {
  import io.circe.parser._

  type ParseJson[T] = String => Either[JsonDecodingError, T]

  implicit def stringDecoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Decoder[String] =
    (inputStream: FiniteInputStream) =>
      Try {
        IOUtils.toString(inputStream, charset)
      } match {
        case Success(string) if string.getBytes.length == inputStream.length =>
          Right(string)
        case Success(string) =>
          Left(IncorrectStreamLengthError(
            new Throwable(
              s"Expected length ${inputStream.length}, actually had length ${string.getBytes.length}")
          ))
        case Failure(err) => Left(StringDecodingError(err))
    }

  implicit val jsonDecoder: Decoder[Json] =
    (inputStream: FiniteInputStream) => {
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
    (inputStream: FiniteInputStream) => {
      val parseJson: ParseJson[T] = fromJson[T](_) match {
        case Failure(err) => Left(JsonDecodingError(err))
        case Success(t)   => Right(t)
      }

      for {
        jsonString <- stringDecoder.fromStream(inputStream)
        result <- parseJson(jsonString)
      } yield result
    }

  implicit val streamDecoder: Decoder[FiniteInputStream] =
    (inputStream: FiniteInputStream) => Right(inputStream)
}
