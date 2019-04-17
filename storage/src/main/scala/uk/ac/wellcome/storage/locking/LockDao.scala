package uk.ac.wellcome.storage.locking

// LockDao

trait LockDao[Ident, ContextId, Out] {
  def lock(id: Ident, ctxId: ContextId): Either[LockFailure[Ident], Out]
  def unlock(ctxId: ContextId): Either[UnlockFailure[ContextId, Ident], Unit]
  def handleUnlockError(failed: UnlockFailure[ContextId, Ident]): Unit = ()
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable)
  extends FailedLockDaoOp

case class UnlockFailure[ContextId, Ident](ctxId: ContextId, ids: List[Ident], e: Throwable)
  extends FailedLockDaoOp


// LockingService

sealed trait FailedLockingOp

case class FailedLock[ContextId, Ident](ctxId: ContextId, lockFailures: List[LockFailure[Ident]])
  extends FailedLockingOp

case class FailedUnlock[ContextId, Ident](ctxId: ContextId, ids: List[Ident], e: Throwable) extends FailedLockingOp

case class FailedProcess[ContextId](ctxId: ContextId, e: Throwable) extends FailedLockingOp