package uk.ac.wellcome.storage.locking

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FunSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.runtime.BoxedUnit

trait LockDaoTestCases[Ident, ContextId, LockDaoContext]
    extends FunSpec
    with Matchers
    with EitherValues
    with ScalaFutures
    with LockDaoFixtures[Ident, ContextId, LockDaoContext] {
  describe("behaves as a LockDao") {
    it("behaves correctly") {
      withLockDaoContext { lockDaoContext =>
        withLockDao(lockDaoContext) { lockDao =>
          val id1 = createIdent
          val id2 = createIdent

          val contextId1 = createContextId
          val contextId2 = createContextId

          lockDao.lock(id1, contextId1) shouldBe a[Right[_, _]]
          lockDao.lock(id2, contextId2) shouldBe a[Right[_, _]]

          lockDao.lock(id1, contextId2) shouldBe a[Left[_, _]]

          lockDao.lock(id1, contextId1) shouldBe a[Right[_, _]]

          lockDao.unlock(contextId1) shouldBe a[Right[_, _]]
          lockDao.unlock(contextId1) shouldBe a[Right[_, _]]

          lockDao.lock(id1, contextId2) shouldBe a[Right[_, _]]
          lockDao.lock(id2, contextId1) shouldBe a[Left[_, _]]
        }
      }
    }

    it("adds new locks to an existing context") {
      withLockDao { lockDao =>
        val contextId = createContextId

        val id1 = createIdent
        val id2 = createIdent

        lockDao.lock(id1, contextId).right.value.id shouldBe id1
        lockDao.lock(id2, contextId).right.value.id shouldBe id2
      }
    }

    it("blocks locking the same ID in different contexts") {
      withLockDao { lockDao =>
        val id = createIdent

        val contextId1 = createContextId
        val contextId2 = createContextId

        lockDao.lock(id, contextId1).right.value.id shouldBe id

        lockDao.lock(id, contextId2).left.value shouldBe a[LockFailure[_]]
      }
    }

    it("unlocks a locked context and can re-lock in a different context") {
      withLockDao { lockDao =>
        val id = createIdent

        val contextId1 = createContextId
        val contextId2 = createContextId

        lockDao.lock(id, contextId1).right.value.id shouldBe id

        lockDao.unlock(contextId1).right.value shouldBe a[BoxedUnit]

        lockDao.lock(id, contextId2).right.value.id shouldBe id
      }
    }

    it("allows one success if multiple processes try to lock the same ID") {
      withLockDao { lockDao =>
        val lockUnlockCycles = 5
        val parallelism = 5

        // All locks/unlocks except one will fail in each cycle
        val expectedFail = parallelism - 1

        (1 to lockUnlockCycles).foreach { _ =>
          val id = createIdent
          val countDownLatch = new CountDownLatch(parallelism)

          val eventualLocks = Future.sequence {
            (1 to parallelism).map { _ =>
              Future {
                countDownLatch.countDown()
                lockDao.lock(id, createContextId)
              }
            }
          }

          countDownLatch.await(5, TimeUnit.SECONDS)

          whenReady(eventualLocks) { lockAttempts =>
            lockAttempts.count {
              _.isRight
            } shouldBe 1
            lockAttempts.count {
              _.isLeft
            } shouldBe expectedFail
          }
        }
      }
    }
  }
}
