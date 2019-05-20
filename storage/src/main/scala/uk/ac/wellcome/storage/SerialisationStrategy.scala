package uk.ac.wellcome.storage

import java.io.{ByteArrayInputStream, InputStream}

import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.codec.digest.DigestUtils

import scala.io.Source
import scala.util.{Success, Try}

case class StorageKey(value: String) extends AnyVal
case class StorageStream(inputStream: InputStream, storageKey: StorageKey)

trait SerialisationStrategy[T] {
  def toStream(t: T): StorageStream
  def fromStream(input: InputStream): Try[T]
}

object SerialisationStrategy {

  def apply[T](
    implicit strategy: SerialisationStrategy[T]): SerialisationStrategy[T] =
    strategy

  implicit def stringStrategy: SerialisationStrategy[String] =
    new SerialisationStrategy[String] {
      def toStream(t: String): StorageStream = {
        val key = StorageKey(hash(t))
        val input = new ByteArrayInputStream(t.getBytes)

        StorageStream(input, key)
      }

      def fromStream(input: InputStream) =
        Success(Source.fromInputStream(input).mkString)
    }

  implicit def jsonStrategy(
    implicit stringStrategy: SerialisationStrategy[String]
  ): SerialisationStrategy[Json] =
    new SerialisationStrategy[Json] {
      def toStream(t: Json): StorageStream =
        stringStrategy.toStream(t.noSpaces)

      def fromStream(input: InputStream): Try[Json] =
        stringStrategy.fromStream(input).flatMap { parse(_).toTry }
    }

  implicit def typeStrategy[T](
    implicit jsonStorageStrategy: SerialisationStrategy[Json],
    encoder: Encoder[T],
    decoder: Decoder[T]
  ): SerialisationStrategy[T] = new SerialisationStrategy[T] {
    def toStream(t: T): StorageStream =
      jsonStorageStrategy.toStream(t.asJson)

    def fromStream(input: InputStream): Try[T] =
      jsonStorageStrategy.fromStream(input).flatMap(_.as[T].toTry)
  }

  implicit def streamStrategy(
    implicit stringStrategy: SerialisationStrategy[String]
  ): SerialisationStrategy[InputStream] =
    new SerialisationStrategy[InputStream] {
      def toStream(t: InputStream): StorageStream =
        stringStrategy.toStream(Source.fromInputStream(t).mkString)

      def fromStream(input: InputStream) = Success(input)
    }

  private def hash(s: String) =
    DigestUtils.sha256Hex(s)
}
