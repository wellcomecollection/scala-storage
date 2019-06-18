package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.{Charset, StandardCharsets}

import io.circe
import io.circe.Json
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.fromJson
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

trait Decoder[T] {
  type DecoderResult[S] = Either[DecoderError, S]

  def fromStream(inputStream: InputStream with FiniteStream): DecoderResult[T]
}

object DecoderInstances {
  import io.circe.parser._

  type ParseJson[T] = String => Either[JsonDecodingError, T]

  implicit val bytesDecoder: Decoder[Array[Byte]] =
    (inputStream: InputStream with FiniteStream) =>
      Try {
        IOUtils.toByteArray(inputStream)
      } match {
        case Success(bytes) if bytes.length == inputStream.length =>
          Right(bytes)
        case Success(bytes) =>
          Left(
            IncorrectStreamLengthError(
              new Throwable(
                s"Expected length ${inputStream.length}, actually had length ${bytes.length}")
            ))
        case Failure(err) => Left(ByteDecodingError(err))
    }

  implicit def stringDecoder(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Decoder[String] =
    (inputStream: InputStream with FiniteStream) =>
      bytesDecoder.fromStream(inputStream).flatMap { bytes =>
        // TODO: We don't have a test for this String construction failing, because
        // we can't find a sequence of bugs that triggers an exception!
        //
        // We should either satisfy ourselves that this code can't throw, or add
        // a test for this case.
        Try { new String(bytes, charset) } match {
          case Success(string) => Right(string)
          case Failure(err)    => Left(StringDecodingError(err))
        }
    }

  implicit val jsonDecoder: Decoder[Json] =
    (inputStream: InputStream with FiniteStream) => {
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
    (inputStream: InputStream with FiniteStream) => {
      val parseJson: ParseJson[T] = fromJson[T](_) match {
        case Failure(err) => Left(JsonDecodingError(err))
        case Success(t)   => Right(t)
      }

      for {
        jsonString <- stringDecoder.fromStream(inputStream)
        result <- parseJson(jsonString)
      } yield result
    }

  implicit val streamDecoder: Decoder[InputStream with FiniteStream] =
    (inputStream: InputStream with FiniteStream) => Right(inputStream)
}
