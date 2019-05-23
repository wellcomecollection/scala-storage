package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.Charset

import io.circe
import io.circe.{Json, ParsingFailure}
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil.{toJson, fromJson}

import scala.util.{Failure, Success, Try}

trait StorageError {
  val e: Throwable
}

sealed trait RetrievalError extends StorageError

sealed trait EncoderError extends RetrievalError

trait Encoder[T] {
  def toStream(t:T): Either[EncoderError, InputStream]
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
      case Failure(err) => Left(new EncoderError {
        override val e: Throwable = err
      })
    }

  implicit val jsonEncoder: Encoder[Json] =
    (t: Json) => stringEncoder.toStream(t.noSpaces)

  implicit def typeEncoder[T](implicit enc: circe.Encoder[T]): Encoder[T] =
    (t: T) => toJson(t) match {
      case Success(jsonString) => stringEncoder.toStream(jsonString)
      case Failure(err) => Left(new EncoderError {
        override val e: Throwable = err
      })
    }

  implicit val streamEncoder: Encoder[InputStream] =
    (t: InputStream) => Right(t)
}

sealed trait WriteError extends StorageError

sealed trait DecoderError extends WriteError

trait Decoder[T] {
  def fromStream(inputStream: InputStream): Either[DecoderError, T]
}

object DecoderInstances {
  import io.circe.parser._

  implicit val stringDecoder = new Decoder[String] {
    override def fromStream(inputStream: InputStream): Either[DecoderError, String] = Try {
      IOUtils.toString(inputStream, Charset.defaultCharset)
    } match {
      case Success(string) => Right(string)
      case Failure(err) => Left(new DecoderError {
        override val e: Throwable = err
      })
    }
  }

  implicit val jsonDecoder = new Decoder[Json] {
    override def fromStream(inputStream: InputStream): Either[DecoderError, Json] = {
      val parseJson = parse(_) match {
        case Left(err) => Left(new DecoderError {
          override val e: Throwable = err
        })
        case Right(json) => Right(json)
      }

      for {
        jsonString <- stringDecoder.fromStream(inputStream)
        result <- parseJson(jsonString)
      } yield result
    }
  }

  implicit def typeDecoder[T](implicit dec: circe.Decoder[T]) = new Decoder[T] {
    override def fromStream(inputStream: InputStream): Either[DecoderError, T] = {
      val parseJson = fromJson[T](_) match {
        case Failure(err) => Left(new DecoderError {
          override val e: Throwable = err
        })
        case Success(t) => Right(t)
      }

      for {
        jsonString <- stringDecoder.fromStream(inputStream)
        result <- parseJson(jsonString)
      } yield result
    }
  }

  implicit val streamDecoder = new Decoder[InputStream] {
    override def fromStream(inputStream: InputStream): Either[DecoderError, InputStream] = Right(inputStream)
  }
}