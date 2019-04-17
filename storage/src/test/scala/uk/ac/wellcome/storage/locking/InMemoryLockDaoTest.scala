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

    dao.lock("id","ctx") shouldBe Right(())
    dao.lock("id2","ctx2") shouldBe Right(())

    dao.lock("id","different").left.value.e.getMessage shouldEqual
      s"Unable to lock id in different, already locked with context ctx"

    dao.lock("id","ctx") shouldBe Right(())

    dao.unlock("ctx") shouldBe Right(())
    dao.unlock("ctx") shouldBe Right(())

    dao.lock("id","different") shouldBe Right(())
    dao.lock("id2","different").left.value.e.getMessage shouldEqual
      s"Unable to lock id2 in different, already locked with context ctx2"

  }
}
