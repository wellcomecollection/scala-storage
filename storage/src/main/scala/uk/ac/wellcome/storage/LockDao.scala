package uk.ac.wellcome.storage

trait LockDao[LockDaoIdent, LockDaoContextId] {
  type Ident = LockDaoIdent
  type ContextId = LockDaoContextId

  type LockResult = Either[LockFailure[Ident], Lock[Ident, ContextId]]
  type UnlockResult = Either[UnlockFailure[ContextId], Unit]

  def lock(id: Ident, ctxId: ContextId): LockResult
  def unlock(ctxId: ContextId): UnlockResult
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable) extends FailedLockDaoOp

case class UnlockFailure[ContextId](ctxId: ContextId, e: Throwable)
    extends FailedLockDaoOp
