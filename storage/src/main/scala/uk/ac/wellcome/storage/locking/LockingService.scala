package uk.ac.wellcome.storage.locking

import cats._
import cats.data._
import grizzled.slf4j.Logging

import scala.language.higherKinds

trait LockingService[Ident, ContextId, Out, OutMonad[_], LockDaoImpl <: LockDao[Ident, ContextId, _]] extends Logging {

  import cats.implicits._

  implicit val lockDao: LockDaoImpl

  type DaoLock = Either[LockFailure[Ident], Unit]
  type LockFailureSet = Set[LockFailure[Ident]]

  type Lock = Either[FailedLockingServiceOp, ContextId]
  type Process = Either[FailedLockingServiceOp, Out]

  type OutMonadError = MonadError[OutMonad, Throwable]

  def withLocks(ids: Set[Ident])(
    f: => OutMonad[Out]
  )(implicit m: OutMonadError): OutMonad[Process] = {

    val getLocksWithContextId = getLocks(createContextId)_

    val eitherT = for {
      ctxId <- EitherT.fromEither[OutMonad](
        getLocksWithContextId(ids))

      out <- EitherT(safeF(ctxId)(f))
    } yield out

    eitherT.value
  }

  private def safeF(ctxId: ContextId)(
    f: => OutMonad[Out]
  )(implicit monadError: OutMonadError): OutMonad[Process] = {
    val partialF = f.map(o => {
      debug(s"Processing $ctxId (got $o)")
      unlock(ctxId)
      Either.right[FailedLockingServiceOp, Out](o)
    })

    monadError.handleError(partialF) { e =>
      unlock(ctxId)
      Either.left[FailedLockingServiceOp, Out](
        FailedProcess[ContextId](ctxId, e)
      )
    }
  }

  private def getFailedLocks(locks: Set[DaoLock]): LockFailureSet =
    locks.foldLeft(Set.empty[LockFailure[Ident]]) { (acc, o) =>
      o match {
        case Right(_) => acc
        case Left(failedLock) => acc + failedLock
      }
    }

  private def getLocks(ctxId: ContextId)(ids: Set[Ident]): Lock = {
    val locks = ids.map(lock(_, ctxId))
    val failedLocks = getFailedLocks(locks)

    if (failedLocks.isEmpty) {
      Right(ctxId)
    } else {
      unlock(ctxId)
      Left(FailedLock(ctxId, failedLocks))
    }
  }

  protected def createContextId: ContextId

  protected def lock(id: Ident, ctxId: ContextId): DaoLock =
    lockDao.lock(id, ctxId).map(_ => ())

  protected def unlock(ctxId: ContextId): Unit =
    lockDao.unlock(ctxId).leftMap(lockDao.handleUnlockError)

}

sealed trait FailedLockingServiceOp

case class FailedLock[ContextId, Ident](ctxId: ContextId, lockFailures: Set[LockFailure[Ident]])
  extends FailedLockingServiceOp

case class FailedUnlock[ContextId, Ident](ctxId: ContextId, ids: List[Ident], e: Throwable) extends FailedLockingServiceOp

case class FailedProcess[ContextId](ctxId: ContextId, e: Throwable) extends FailedLockingServiceOp