package uk.ac.wellcome.storage.locking

import java.util.UUID

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.memory.{MemoryLockDao, PermanentLock}

import scala.util.Random

class MemoryLockDaoTest
  extends FunSpec
    with Matchers
    with EitherValues {

  it("behaves correctly") {
    val dao = new MemoryLockDao[String, UUID] {}

    val id1 = createId
    val id2 = createId

    val contextId1 = createContextId
    val contextId2 = createContextId

    dao.lock(id1, contextId1) shouldBe a[Right[_,_]]
    dao.lock(id2, contextId2) shouldBe a[Right[_,_]]

    dao.lock(id1, contextId2).left.value.e.getMessage shouldBe
      s"Failed to lock <$id1> in context <$contextId2>; already locked as <$contextId1>"

    dao.lock(id1, contextId1) shouldBe a[Right[_,_]]

    dao.unlock(contextId1) shouldBe a[Right[_,_]]
    dao.unlock(contextId1) shouldBe a[Right[_,_]]

    dao.lock(id1, contextId2) shouldBe a[Right[_,_]]
    dao.lock(id2, contextId1).left.value.e.getMessage shouldBe
      s"Failed to lock <$id2> in context <$contextId1>; already locked as <$contextId2>"
  }

  it("records a history of locks") {
    val dao = new MemoryLockDao[String, UUID] {}

    val id1 = createId

    val contextId1 = createContextId
    val contextId2 = createContextId

    dao.lock(id1, contextId1)
    dao.unlock(contextId1)
    dao.lock(id1, contextId2)

    dao.history shouldBe List(
      PermanentLock(id1, contextId1),
      PermanentLock(id1, contextId2),
    )
  }

  def createId: String = Random.alphanumeric take 8 mkString

  def createContextId: UUID = UUID.randomUUID()
}
