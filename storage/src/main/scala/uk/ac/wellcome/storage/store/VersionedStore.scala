package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima

import scala.util.{Failure, Success, Try}

class VersionedStore[Id, V, T](
  val store: Store[Version[Id, V], T] with Maxima[Id, V]
)(implicit N: Numeric[V], O: Ordering[V])
    extends Store[Version[Id, V], T]
    with Logging {

  type StorageEither = Either[StorageError, Identified[Version[Id, V], T]]
  type UpdateEither = Either[UpdateError, Identified[Version[Id, V], T]]
  type UpdateFunctionEither = Either[UpdateFunctionError, T]

  type UpdateFunction = T => UpdateFunctionEither

  private val zero = N.zero
  private def increment(v: V): V = N.plus(v, N.one)

  private def nextVersionFor(id: Id): Either[ReadError, V] =
    store.max(id).map(increment)

  private val matchErrors: PartialFunction[
    StorageEither,
    UpdateEither
  ] = {
    case Left(err: NoVersionExistsError) =>
      Left(UpdateNoSourceError(err))

    case Left(err: ReadError) =>
      Left(UpdateReadError(err))

    case Left(err: WriteError) =>
      Left(UpdateWriteError(err))

    case Left(err: UpdateError) =>
      Left(err)

    case Right(r) =>
      Right(r)
  }

  private def safeF(f: UpdateFunction)(t: T): UpdateFunctionEither =
    Try(f(t)) match {
      case Success(value) => value
      case Failure(e)     => Left(UpdateUnexpectedError(e))
    }

  def init(id: Id)(t: T): WriteEither =
    put(Version(id, N.zero))(t)

  def upsert(id: Id)(t: T)(f: UpdateFunction): UpdateEither =
    update(id)(f) match {
      case Left(UpdateNoSourceError(_)) =>
        matchErrors.apply(put(Version(id, N.zero))(t))
      case default => default
    }

  def update(id: Id)(f: UpdateFunction): UpdateEither =
    matchErrors.apply(for {
      latest <- getLatest(id)
      updatedT <- safeF(f)(latest.identifiedT)
      result <- put(Version(id, increment(latest.id.version)))(updatedT)
    } yield result)

  def get(id: Version[Id, V]): ReadEither =
    store.get(id) match {
      case r @ Right(_) => r
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
    store.max(id.id) match {
      case Right(latestV) if O.gt(latestV, id.version) =>
        Left(HigherVersionExistsError())
      case Right(latestV) if latestV == id.version =>
        Left(VersionAlreadyExistsError())
      case _ =>
        store
          .put(id)(t)
    }

  def putLatest(id: Id)(t: T): WriteEither = {
    val result = nextVersionFor(id) match {
      case Right(v) =>
        put(Version(id, v))(t)
      case Left(NoMaximaValueError(_)) =>
        put(Version(id, zero))(t)
      case Left(err: StorageError) =>
        Left(StoreWriteError(err.e))
    }

    result match {
      case Right(value) => Right(value)

      // We need to handle the case where two processes call putLatest() simultaneously,
      // and the version checking logic in put() throws an error.
      //
      // See VersionedStoreRaceConditionsTest for examples of how this can occur.
      case Left(_: VersionAlreadyExistsError) =>
        Left(
          new StoreWriteError(
            new Throwable(s"Another process wrote to id=$id simultaneously!")) with RetryableError
        )
      case Left(err) => Left(err)
    }
  }

}
