package uk.ac.wellcome.storage.locking

trait LockDao[Ident, ContextId, Out] {
  type Lock = Either[LockFailure[Ident], Out]
  type Unlock = Either[UnlockFailure[ContextId, Ident], Unit]

  type UnlockFail = UnlockFailure[ContextId, Ident]

  def lock(id: Ident, ctxId: ContextId): Lock
  def unlock(ctxId: ContextId): Unlock

  def handleUnlockError(failed: UnlockFail): Unit = ()
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable)
  extends FailedLockDaoOp

case class UnlockFailure[ContextId, Ident](ctxId: ContextId, ids: List[Ident], e: Throwable)
  extends FailedLockDaoOp