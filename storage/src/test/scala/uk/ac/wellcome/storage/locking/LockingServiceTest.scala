package uk.ac.wellcome.storage.locking

import cats.implicits._
import org.scalatest._
import uk.ac.wellcome.storage.fixtures.LockingServiceFixtures

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
    withLockingService() { service =>
      assertLockSuccess(service.withLocks(lockIds)(f))
    }
  }

  it("fails if you try to re-lock the same identifiers twice") {
    withLockingService() { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        assertFailedLock(service.withLocks(lockIds)(f), lockIds)

        f
      })
    }
  }

  it("fails if you try to re-lock an already locked identifier") {
    withLockingService() { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        assertFailedLock(
          service.withLocks(overlappingLockIds)(f),
          commonLockIds)

        f
      })
    }
  }

  it("allows multiple, nested locks on different identifiers") {
    withLockingService() { service =>
      assertLockSuccess(service.withLocks(lockIds) {
        assertLockSuccess(service.withLocks(differentLockIds)(f))

        f
      })
    }
  }

  it("unlocks a context set when done, and allows you to re-lock them") {
    withLockingService() { service =>
      assertLockSuccess(service.withLocks(lockIds)(f))
      assertLockSuccess(service.withLocks(lockIds)(f))
    }
  }

  it("unlocks a context set when a result throws a Throwable") {
    withLockingService() { service =>
      assertFailedProcess(
        service.withLocks(lockIds)(fError), expectedError)
      assertLockSuccess(
        service.withLocks(lockIds)(f))
    }
  }

  it("unlocks a context set when a partial lock is acquired") {
    withLockingService() { service =>
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

  // TODO:
  // - When only some locks are acquired (locks should be released).
  // - When unlock fails, ensure LockDao is called

  // - When exception thrown during lock
  // - When exception thrown during unlock
}
