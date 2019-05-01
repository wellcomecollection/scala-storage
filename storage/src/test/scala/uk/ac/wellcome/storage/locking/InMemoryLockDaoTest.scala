package uk.ac.wellcome.storage.locking

import java.util.UUID

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.LockDao
import uk.ac.wellcome.storage.fixtures.LockDaoFixtures

import scala.util.Random

class InMemoryLockDaoTest
  extends FunSpec
    with Matchers
    with EitherValues
    with LockDaoFixtures {

  it("behaves correctly") {
    val dao: LockDao[String, UUID] = createBetterInMemoryLockDao

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

  def createId: String = Random.alphanumeric take 8 mkString

  def createContextId: UUID = UUID.randomUUID()
}
