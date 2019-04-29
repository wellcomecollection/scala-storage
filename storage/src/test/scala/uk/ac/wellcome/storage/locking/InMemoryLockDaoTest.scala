package uk.ac.wellcome.storage.locking

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.LockDaoFixtures

class InMemoryLockDaoTest
  extends FunSpec
    with Matchers
    with EitherValues
    with LockDaoFixtures{

  it("behaves correctly") {
    val dao = createInMemoryLockDao

    dao.lock("id","ctx") shouldBe a[Right[_,_]]
    dao.lock("id2","ctx2") shouldBe a[Right[_,_]]

    dao.lock("id","different").left.value.e.getMessage shouldEqual
      s"Failed lock id in different, locked: ctx"

    dao.lock("id","ctx") shouldBe a[Right[_,_]]

    dao.unlock("ctx") shouldBe a[Right[_,_]]
    dao.unlock("ctx") shouldBe a[Right[_,_]]

    dao.lock("id","different") shouldBe a[Right[_,_]]
    dao.lock("id2","different").left.value.e.getMessage shouldEqual
      s"Failed lock id2 in different, locked: ctx2"

  }
}
