package uk.ac.wellcome.storage.locking

import cats.implicits._
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.LockingServiceFixtures
import uk.ac.wellcome.storage.locking.memory.PermanentLock

import scala.util.Try

trait LockingServiceTestCases[Ident, ContextId, LockDaoContext] extends FunSpec with Matchers with LockingServiceFixtures[Ident, ContextId, LockDaoContext] {
  def getCurrentLocks(lockDao: LockDaoStub, lockDaoContext: LockDaoContext): Set[Ident]

  val commonLockIds = Set(createIdent)
  val nonOverlappingLockIds = Set(createIdent, createIdent)

  val lockIds: Set[Ident] = Set(createIdent, createIdent) ++ commonLockIds

  val overlappingLockIds: Set[Ident] = commonLockIds ++ nonOverlappingLockIds
  val differentLockIds = Set(createIdent, createIdent, createIdent)

  def withLockingService[R](testWith: TestWith[LockingServiceStub, R]): R =
    withLockDaoContext { lockDaoContext =>
      withLockDao(lockDaoContext) { lockDao =>
        withLockingService(lockDao) { service =>
          testWith(service)
        }
      }
    }

  describe("behaves as a LockingService") {
    it("acquires a lock successfully, and returns the result") {
      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          withLockingService(lockDao) { service =>
            assertLockSuccess(service.withLocks(lockIds) {
              getCurrentLocks(lockDao, lockDaoContext) shouldBe lockIds
              f
            })
          }
        }
      }
    }

    it("allows locking a single identifier") {
      val id = createIdent

      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          withLockingService(lockDao) { service =>
            assertLockSuccess(service.withLock(id) {
              getCurrentLocks(lockDao, lockDaoContext) shouldBe Set(id)
              f
            })
          }
        }
      }
    }

    it("fails if you try to re-lock the same identifiers twice") {
      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          withLockingService(lockDao) { service =>
            assertLockSuccess(service.withLocks(lockIds) {
              assertFailedLock(service.withLocks(lockIds)(f), lockIds)

              // Check the original locks were preserved
              getCurrentLocks(lockDao, lockDaoContext) shouldBe lockIds

              f
            })
          }
        }
      }
    }

    it("fails if you try to re-lock an already locked identifier") {
      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          withLockingService(lockDao) { service =>
            assertLockSuccess(service.withLocks(lockIds) {
              assertFailedLock(
                service.withLocks(overlappingLockIds)(f),
                commonLockIds)

              // Check the original locks were preserved
              getCurrentLocks(lockDao, lockDaoContext) shouldBe lockIds

              f
            })
          }
        }
      }
    }

    it("allows multiple, nested locks on different identifiers") {
      withLockingService { service =>
        assertLockSuccess(service.withLocks(lockIds) {
          assertLockSuccess(service.withLocks(differentLockIds)(f))

          f
        })
      }
    }

    it("unlocks a context set when done, and allows you to re-lock them") {
      withLockingService { service =>
        assertLockSuccess(service.withLocks(lockIds)(f))
        assertLockSuccess(service.withLocks(lockIds)(f))
      }
    }

    it("unlocks a context set when a result throws a Throwable") {
      withLockingService { service =>
        assertFailedProcess(
          service.withLocks(lockIds)(fError), expectedError)
        assertLockSuccess(
          service.withLocks(lockIds)(f))
      }
    }

    it("unlocks a context set when a partial lock is acquired") {
      withLockingService { service =>
        assertLockSuccess(service.withLocks(lockIds) {

          assertFailedLock(
            service.withLocks(overlappingLockIds)(f),
            commonLockIds
          )

          assertLockSuccess(
            service.withLocks(nonOverlappingLockIds)(f)
          )

          f
        })
      }
    }

    it("calls the callback if asked to lock an empty set") {
      withLockingService { service =>
        assertLockSuccess(
          service.withLocks(Set.empty)(f)
        )
      }
    }

    it("returns a success even if unlocking fails") {
      val brokenUnlockDao = new LockDao[Ident, ContextId] {
        override def lock(id: Ident, contextId: ContextId): LockResult =
          Right(PermanentLock(id = id, contextId = contextId))

        override def unlock(contextId: ContextId): UnlockResult =
          Left(UnlockFailure(contextId, new Throwable("BOOM!")))
      }

      withLockingService(brokenUnlockDao) { service =>
        assertLockSuccess(
          service.withLocks(lockIds)(f)
        )
      }
    }

    it("releases locks if the callback fails") {
      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          withLockingService(lockDao) { service =>
            val result = service.withLocks(lockIds) {
              Try {
                throw new Throwable("BOOM!")
              }
            }

            result.get.left.value shouldBe a[FailedProcess[_]]
            getCurrentLocks(lockDao, lockDaoContext) shouldBe empty
          }
        }
      }
    }
  }
}
