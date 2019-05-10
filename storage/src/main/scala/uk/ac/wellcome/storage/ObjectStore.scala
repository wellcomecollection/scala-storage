package uk.ac.wellcome.storage

import java.io.InputStream

import uk.ac.wellcome.storage.type_classes.{SerialisationStrategy, StorageStream}

import scala.util.Try

case class KeyPrefix(value: String) extends AnyVal
case class KeySuffix(value: String) extends AnyVal

trait BetterObjectStore[T] {
  implicit val serialisationStrategy: SerialisationStrategy[T]
  implicit val storageBackend: StorageBackend

  def put(namespace: String)(
    input: T,
    keyPrefix: KeyPrefix = KeyPrefix(""),
    keySuffix: KeySuffix = KeySuffix(""),
    userMetadata: Map[String, String] = Map.empty
  ): Try[ObjectLocation] = {
    val prefix = normalizePathFragment(keyPrefix.value)
    val suffix = normalizePathFragment(keySuffix.value)

    for {
      storageStream: StorageStream <- Try {
        serialisationStrategy.toStream(input)
      }
      storageKey = storageStream.storageKey.value
      key = s"$prefix/$storageKey$suffix"
      location = ObjectLocation(namespace, key)
      _ <- storageBackend.put(
        location = location,
        input = storageStream.inputStream,
        metadata = userMetadata
      )
    } yield location
  }

  def get(objectLocation: ObjectLocation): Try[T] =
    for {
      input <- storageBackend.get(objectLocation)
      t <- serialisationStrategy.fromStream(input)
      _ <- Try {
        if (t.isInstanceOf[InputStream]) () else input.close()
      }
    } yield t

  private def normalizePathFragment(prefix: String): String =
    prefix
      .stripPrefix("/")
      .stripSuffix("/")
}

object BetterObjectStore {
  def apply[T](implicit store: BetterObjectStore[T]): BetterObjectStore[T] =
    store

  implicit def createObjectStore[T](
    implicit strategy: SerialisationStrategy[T],
    backend: StorageBackend): BetterObjectStore[T] = new BetterObjectStore[T] {
      override implicit val serialisationStrategy: SerialisationStrategy[T] = strategy
      override implicit val storageBackend: StorageBackend = backend
    }
}
