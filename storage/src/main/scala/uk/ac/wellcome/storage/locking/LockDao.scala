package uk.ac.wellcome.storage.locking

trait LockDao[LockDaoIdent, LockDaoContextId] {

  type Ident = LockDaoIdent
  type ContextId = LockDaoContextId

  type Lock = Either[LockFailure[Ident], ExpiringLock]
  type Unlock = Either[UnlockFailure[ContextId], Unit]

  def lock(id: Ident, ctxId: ContextId): Lock
  def unlock(ctxId: ContextId): Unlock
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable)
  extends FailedLockDaoOp

case class UnlockFailure[ContextId](ctxId: ContextId, e: Throwable)
  extends FailedLockDaoOp