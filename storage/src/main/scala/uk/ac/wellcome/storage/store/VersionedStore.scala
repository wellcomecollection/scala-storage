package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima

class VersionedStore[Id, V, T](
                                store: Store[Version[Id, V], T] with Maxima[Id, V]
                              )(implicit N: Numeric[V], O: Ordering[V])
  extends Store[Version[Id, V], T]
    with Logging {

  type UpdateEither = Either[UpdateError, Identified[Version[Id, V], T]]

  val zero = N.zero
  def increment(v: V): V = N.plus(v, N.one)

  private def nextVersionFor(id: Id): Either[ReadError, V] =
    store.max(id).map(increment)

  private val matchErrors: PartialFunction[
    Either[StorageError, Identified[Version[Id, V], T]],
    Either[UpdateError, Identified[Version[Id, V], T]],
    ] = {
    case Left(err: NoVersionExistsError) => Left(UpdateNoSourceError(err.e))
    case Left(err: ReadError) => Left(UpdateReadError(err.e))
    case Left(err: WriteError) => Left(UpdateWriteError(err.e))
    case Right(r) => Right(r)
  }

  def init(id: Id)(t: T): WriteEither =
    put(Version(id, N.zero))(t)

  def upsert(id: Id)(t: T)(f: T => T): UpdateEither =
    update(id)(f) match {
      case Left(UpdateNoSourceError(_)) =>
        matchErrors.apply(put(Version(id, N.zero))(t))
      case default => default
    }

  def update(id: Id)(f: T => T): UpdateEither = matchErrors.apply(
    getLatest(id).flatMap { identified =>
      val v = N.plus(identified.id.version, N.one)
      put(Version(id, v))(f(identified.identifiedT))
    })

  def get(id: Version[Id, V]): ReadEither =
    store.get(id) match {
      case r@Right(_) => r
      case Left(DoesNotExistError(_)) =>
        Left(NoVersionExistsError())
      case Left(err) => Left(err)
    }

  def getLatest(id: Id): ReadEither =
    store.max(id) match {
      case Right(v) => get(Version(id, v))
      case Left(NoMaximaValueError(_)) =>
        Left(NoVersionExistsError())
      case Left(err) => Left(err)
    }

  def put(id: Version[Id, V])(t: T): WriteEither =
    store.max(id.id: Id) match {
      case Right(latestV) if O.gt(latestV, id.version) =>
        Left(HigherVersionExistsError())
      case Right(latestV) if latestV == id.version =>
        Left(VersionAlreadyExistsError())
      case _ => store
        .put(id)(t)
    }

  def putLatest(id: Id)(t: T): WriteEither =
    nextVersionFor(id) match {
      case Right(v) =>
        put(Version(id, v))(t)
      case Left(NoMaximaValueError(_)) =>
        put(Version(id, zero))(t)
      case Left(err: StorageError) =>
        Left(StoreWriteError(err.e))
    }
}