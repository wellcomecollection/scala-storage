package uk.ac.wellcome.storage.locking

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