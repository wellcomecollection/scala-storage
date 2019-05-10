package uk.ac.wellcome.storage

import java.io.InputStream

import uk.ac.wellcome.storage.type_classes.{SerialisationStrategy, StorageStream}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class KeyPrefix(value: String) extends AnyVal
case class KeySuffix(value: String) extends AnyVal

trait BetterObjectStore[T] {
  implicit val serialisationStrategy: SerialisationStrategy[T]
  implicit val backend: StorageBackend

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
      _ <- backend.put(
        location = location,
        input = storageStream.inputStream,
        metadata = userMetadata
      )
    } yield location
  }

  def get(objectLocation: ObjectLocation): Try[T] =
    for {
      input <- backend.get(objectLocation)
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




trait ObjectStore[T] {
  def put(namespace: String)(
    input: T,
    keyPrefix: KeyPrefix = KeyPrefix(""),
    keySuffix: KeySuffix = KeySuffix(""),
    userMetadata: Map[String, String] = Map()
  ): Future[ObjectLocation]

  def get(objectLocation: ObjectLocation): Future[T]
}

object ObjectStore {

  private def normalizePathFragment(prefix: String) =
    prefix
      .stripPrefix("/")
      .stripSuffix("/")

  def apply[T](implicit store: ObjectStore[T]): ObjectStore[T] =
    store

  implicit def createObjectStore[T, R <: StorageBackend](
    implicit storageStrategy: SerialisationStrategy[T],
    storageBackend: R,
    ec: ExecutionContext): ObjectStore[T] = new ObjectStore[T] {
    def put(namespace: String)(
      t: T,
      keyPrefix: KeyPrefix = KeyPrefix(""),
      keySuffix: KeySuffix = KeySuffix(""),
      userMetadata: Map[String, String] = Map()
    ): Future[ObjectLocation] = {
      val storageStream = storageStrategy.toStream(t)

      val prefix = normalizePathFragment(keyPrefix.value)
      val suffix = normalizePathFragment(keySuffix.value)
      val storageKey = storageStream.storageKey.value

      val key = s"$prefix/$storageKey$suffix"

      val location = ObjectLocation(namespace, key)

      val stored = storageBackend.put(
        location,
        storageStream.inputStream,
        userMetadata
      )

      Future.fromTry { stored }.map(_ => location)
    }

    def get(objectLocation: ObjectLocation): Future[T] = {
      for {
        input <- Future.fromTry { storageBackend.get(objectLocation) }
        a <- Future.fromTry(storageStrategy.fromStream(input))
        _ <- Future.fromTry(Try {
          if (a.isInstanceOf[InputStream]) () else input.close()
        })
      } yield a
    }
  }
}
