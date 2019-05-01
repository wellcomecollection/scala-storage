package uk.ac.wellcome.storage.locking

import java.util.UUID

import cats.implicits._
import org.scalatest._
import uk.ac.wellcome.storage.{FailedProcess, LockDao, UnlockFailure}
import uk.ac.wellcome.storage.fixtures.{InMemoryLockDao, LockingServiceFixtures, PermanentLock}

import scala.util.Try

class LockingServiceTest
  extends FunSpec
    with Matchers
    with LockingServiceFixtures {

  val commonLockIds = Set("c")
  val nonOverlappingLockIds = Set("g", "h")

  val lockIds = Set("a", "b") ++ commonLockIds

  val overlappingLockIds = commonLockIds ++ nonOverlappingLockIds
  val differentLockIds = Set("d", "e", "f")

  it("acquires a lock successfully, and returns the result") {
    val lockDao = new InMemoryLockDao()

    withLockingService(lockDao) { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        lockDao.getCurrentLocks shouldBe lockIds
        f
      })
    }
  }

  it("allows locking a single identifier") {
    val lockDao = new InMemoryLockDao()

    withLockingService(lockDao) { service =>
      assertLockSuccess(service.withLock("a") {
        lockDao.getCurrentLocks shouldBe Set("a")
        f
      })
    }
  }

  it("fails if you try to re-lock the same identifiers twice") {
    val lockDao = new InMemoryLockDao()

    withLockingService(lockDao) { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        assertFailedLock(service.withLocks(lockIds)(f), lockIds)

        // Check the original locks were preserved
        lockDao.getCurrentLocks shouldBe lockIds

        f
      })
    }
  }

  it("fails if you try to re-lock an already locked identifier") {
    val lockDao = new InMemoryLockDao()

    withLockingService(lockDao) { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        assertFailedLock(
          service.withLocks(overlappingLockIds)(f),
          commonLockIds)

        // Check the original locks were preserved
        lockDao.getCurrentLocks shouldBe lockIds

        f
      })
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
    val brokenUnlockDao = new LockDao[String, UUID] {
      override def lock(id: String, contextId: UUID): LockResult =
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
    val lockDao = new InMemoryLockDao()

    withLockingService(lockDao) { service =>
      val result = service.withLocks(lockIds) {
        Try {
          throw new Throwable("BOOM!")
        }
      }

      result.get.left.value shouldBe a[FailedProcess[_]]
      lockDao.getCurrentLocks shouldBe Set.empty
    }
  }
}
