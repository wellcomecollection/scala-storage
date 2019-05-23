package uk.ac.wellcome.storage

import java.io.InputStream
import java.util.UUID

import uk.ac.wellcome.storage.streaming.Codec

import scala.util.Try

case class KeyPrefix(value: String) extends AnyVal
case class KeySuffix(value: String) extends AnyVal

trait ObjectStore[T] {
  implicit val codec: Codec[T]
  implicit val storageBackend: StorageBackend

  protected def createStorageKey =
    UUID.randomUUID().toString

  def put(namespace: String)(
    input: T,
    keyPrefix: KeyPrefix = KeyPrefix(""),
    keySuffix: KeySuffix = KeySuffix(""),
    userMetadata: Map[String, String] = Map.empty
  ): Either[WriteError, Unit] = {
    val prefix = normalizePathFragment(keyPrefix.value)
    val suffix = normalizePathFragment(keySuffix.value)

    val location = ObjectLocation(
      namespace,
      s"$prefix/$createStorageKey$suffix"
    )

    for {
      inputStream <- codec.toStream(input)

      result <- storageBackend.put(
        location = location,
        input = inputStream,
        metadata = userMetadata
      )
    } yield result
  }

  private def ensureStreamClosed(
                                  stream: InputStream
                                )(t: T) = {
    t match {
      case _: InputStream => Right(())
      case _ => Try(stream.close()).toEither
    }
  }

  def get(objectLocation: ObjectLocation) =
    for {
      inputStream <- storageBackend.get(objectLocation)
      t <- codec.fromStream(inputStream)
      _ <- ensureStreamClosed(inputStream)(t)
    } yield t

  private def normalizePathFragment(prefix: String): String =
    prefix
      .stripPrefix("/")
      .stripSuffix("/")
}

object ObjectStore {
  def apply[T](implicit store: ObjectStore[T]): ObjectStore[T] =
    store

  implicit def createObjectStore[T](implicit codecT: Codec[T],
                                    backend: StorageBackend): ObjectStore[T] =
    new ObjectStore[T] {
      override implicit val codec: Codec[T] = codecT
      override implicit val storageBackend: StorageBackend = backend
    }
}
