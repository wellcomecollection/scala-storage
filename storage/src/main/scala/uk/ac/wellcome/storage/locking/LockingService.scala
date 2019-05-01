package uk.ac.wellcome.storage.locking

import cats._
import cats.data._
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{LockDao, LockFailure, UnlockFailure}

import scala.language.higherKinds

trait LockingService[Out, OutMonad[_], LockDaoImpl <: LockDao[_, _]]
    extends Logging {

  import cats.implicits._

  implicit val lockDao: LockDaoImpl

  type DaoLockFailure = LockFailure[lockDao.Ident]
  type DaoUnlockFailure = UnlockFailure[lockDao.ContextId]
  type DaoLockEither = Either[DaoLockFailure, Unit]

  type Lock = Either[FailedLockingServiceOp, lockDao.ContextId]
  type Process = Either[FailedLockingServiceOp, Out]

  type OutMonadError = MonadError[OutMonad, Throwable]

  def withLocks(ids: Set[lockDao.Ident])(
    f: => OutMonad[Out]
  )(implicit m: OutMonadError): OutMonad[Process] = {
    val contextId: lockDao.ContextId = createContextId()

    val eitherT = for {
      ctxId <- EitherT.fromEither[OutMonad](getLocks(contextId)(ids))

      out <- EitherT(safeF(ctxId)(f))
    } yield out

    eitherT.value
  }

  private def safeF(ctxId: lockDao.ContextId)(
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
        FailedProcess[lockDao.ContextId](ctxId, e)
      )
    }
  }

  private def getFailedLocks(locks: Set[lockDao.LockResult]): Set[DaoLockFailure] =
    locks.foldLeft(Set.empty[LockFailure[lockDao.Ident]]) { (acc, o) =>
      o match {
        case Right(_)         => acc
        case Left(failedLock) => acc + failedLock
      }
    }

  private def getLocks(ctxId: lockDao.ContextId)(
    ids: Set[lockDao.Ident]): Lock = {
    val locks = ids.map { lockDao.lock(_, ctxId) }
    val failedLocks = getFailedLocks(locks)

    if (failedLocks.isEmpty) {
      Right(ctxId)
    } else {
      unlock(ctxId)
      Left(FailedLock(ctxId, failedLocks))
    }
  }

  protected def createContextId(): lockDao.ContextId

  protected def unlock(ctxId: lockDao.ContextId): Unit = {
    // Deal with unlocking _nothing_
    val unlock = Option(lockDao.unlock(ctxId))

    if (unlock.isEmpty) {
      warn(s"Found nothing to unlock for $ctxId!")
    }

    unlock.map {
      _.leftMap { error =>
        warn(s"Unable to unlock context $ctxId fully: $error")
      }
    }
  }

}

sealed trait FailedLockingServiceOp

case class FailedLock[ContextId, Ident](ctxId: ContextId,
                                        lockFailures: Set[LockFailure[Ident]])
    extends FailedLockingServiceOp

case class FailedUnlock[ContextId, Ident](ctxId: ContextId,
                                          ids: List[Ident],
                                          e: Throwable)
    extends FailedLockingServiceOp

case class FailedProcess[ContextId](ctxId: ContextId, e: Throwable)
    extends FailedLockingServiceOp
