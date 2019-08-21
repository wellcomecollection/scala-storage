package uk.ac.wellcome.storage.locking

import uk.ac.wellcome.fixtures.TestWith

trait LockDaoFixtures[Ident, ContextId, LockDaoContext] {
  def withLockDaoContext[R](testWith: TestWith[LockDaoContext, R]): R

  def withLockDao[R](context: LockDaoContext)(
    testWith: TestWith[LockDao[Ident, ContextId], R]): R

  def withLockDao[R](testWith: TestWith[LockDao[Ident, ContextId], R]): R =
    withLockDaoContext { lockDaoContext =>
      withLockDao(lockDaoContext) { lockDao =>
        testWith(lockDao)
      }
    }

  def createIdent: Ident
  def createContextId: ContextId
}
