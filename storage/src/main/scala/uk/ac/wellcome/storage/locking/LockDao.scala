package uk.ac.wellcome.storage.locking

/** This trait defines a generic locking dao (data ccess object).
  *
  * The tests include a basic implementation.
  * See: InMemoryLockDao, InMemoryLockDaoTest.
  *
  */
trait LockDao[LockDaoIdent, LockDaoContextId] {
  type Ident = LockDaoIdent
  type ContextId = LockDaoContextId

  type LockResult = Either[LockFailure[Ident], Lock[Ident, ContextId]]
  type UnlockResult = Either[UnlockFailure[ContextId], Unit]

  /** Lock a single ID.
    *
    * The context ID is used to identify the process that wants the lock.
    * Locking an ID twice with the same context ID should be allowed;
    * locking with a different context ID should be an error.
    *
    */
  def lock(id: Ident, contextId: ContextId): LockResult

  /** Release the lock on every ID that was part of this context. */
  def unlock(contextId: ContextId): UnlockResult
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable) extends FailedLockDaoOp

case class UnlockFailure[ContextId](contextId: ContextId, e: Throwable)
    extends FailedLockDaoOp
