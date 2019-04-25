package uk.ac.wellcome.storage.locking

import cats._
import cats.data._

import grizzled.slf4j.Logging

import scala.language.higherKinds

trait LockingService[Ident, ContextId, Out, OutFailure, IdentF[_], OutF[_], LockDaoImpl <: LockDao[Ident, ContextId, _]] extends Logging {

  import cats.implicits._

  implicit val lockDao: LockDaoImpl

  type DaoLock = Either[LockFailure[Ident], Unit]

  type Locks   = Either[FailedLock[ContextId, Ident], ContextId]

  def withLocks(ids: IdentF[Ident])(
    f: => OutF[Out]
  )(implicit
    fuIdentF: Functor[IdentF],
    foIdentF: Foldable[IdentF],
    monadError: MonadError[OutF, Throwable]
  ): OutF[Either[FailedLockingServiceOp, Out]] = {

    (for {
      ctxId <- EitherT.fromEither[OutF](getLocks(ids))
      out <- EitherT(safeF(ctxId)(f))
    } yield out).value
  }

  private def safeF(ctxId: ContextId)(f: => OutF[Out])(implicit monadError: MonadError[OutF, Throwable]) = {

    val partialF = f.map(o => {
      debug(s"Processing $ctxId (got $o)")
      unlock(ctxId)
      Either.right[FailedLockingServiceOp, Out](o)
    })

    monadError.handleError(partialF) {
      case e => {
        unlock(ctxId)
        Either.left[FailedLockingServiceOp, Out](
          FailedProcess[ContextId](ctxId, e)
        )
      }
    }
  }

  private def getFailedLocks(locks: IdentF[DaoLock])(
    implicit F: Foldable[IdentF]
  ): List[LockFailure[Ident]] = {

    val empty = List.empty[LockFailure[Ident]]

    locks.foldLeft(empty) { (acc, o) =>
      o match {
        case Right(_) => acc
        case Left(failedLock) => failedLock :: acc
      }
    }
  }

  private def getLocks(ids: IdentF[Ident])(
    implicit Fu: Functor[IdentF], Fo: Foldable[IdentF]
  ): Locks = {

    val contextId = createContextId
    val locks: IdentF[DaoLock] = ids.map(lock(_, contextId))

    getFailedLocks(locks) match {
      case Nil => Right(contextId)
      case failedLocks => Left(FailedLock(contextId, failedLocks))
    }
  }

  protected def createContextId: ContextId

  protected def lock(id: Ident, ctxId: ContextId): DaoLock = {
    lockDao.lock(id, ctxId).map(_ => ())
  }

  protected def unlock(ctxId: ContextId): Unit = {
    info(s"Unlocking $ctxId")
    lockDao
      .unlock(ctxId)
      .leftMap(lockDao.handleUnlockError)
  }
}

sealed trait FailedLockingServiceOp

case class FailedLock[ContextId, Ident](ctxId: ContextId, lockFailures: List[LockFailure[Ident]])
  extends FailedLockingServiceOp

case class FailedUnlock[ContextId, Ident](ctxId: ContextId, ids: List[Ident], e: Throwable) extends FailedLockingServiceOp

case class FailedProcess[ContextId](ctxId: ContextId, e: Throwable) extends FailedLockingServiceOp