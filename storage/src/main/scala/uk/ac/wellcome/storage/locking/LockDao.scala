package uk.ac.wellcome.storage.locking

import java.time.Instant

trait LockDao[Ident, ContextId] {
  type Lock = Either[LockFailure[Ident], RowLock]
  type Unlock = Either[UnlockFailure[ContextId], Unit]

  def lock(id: Ident, ctxId: ContextId): Lock
  def unlock(ctxId: ContextId): Unlock
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable)
  extends FailedLockDaoOp

case class UnlockFailure[ContextId](ctxId: ContextId, e: Throwable)
  extends FailedLockDaoOp