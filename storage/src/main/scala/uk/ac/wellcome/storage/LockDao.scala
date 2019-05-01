package uk.ac.wellcome.storage

/** This trait defines a generic locking dao (data ccess object).
  *
  * Instances of this trait need to implement two methods:
  *
  *   - lock() locks a single ID.  The caller supplies a context ID, so they
  *     can associate IDs that were locked as part of a bulk operation.
  *   - unlock() unlocks all the IDs that were assigned a given context ID
  *
  * The tests include a basic implementation (see InMemoryLockDao).
  *
  * See also: InMemoryLockDaoTest, which shows the implicit contract on
  * a LockDao.
  *
  */
trait LockDao[LockDaoIdent, LockDaoContextId] {
  type Ident = LockDaoIdent
  type ContextId = LockDaoContextId

  type LockResult = Either[LockFailure[Ident], Lock[Ident, ContextId]]
  type UnlockResult = Either[UnlockFailure[ContextId], Unit]

  def lock(id: Ident, contextId: ContextId): LockResult
  def unlock(contextId: ContextId): UnlockResult
}

sealed trait FailedLockDaoOp

case class LockFailure[Ident](id: Ident, e: Throwable) extends FailedLockDaoOp

case class UnlockFailure[ContextId](ctxId: ContextId, e: Throwable)
    extends FailedLockDaoOp
