package uk.ac.wellcome.storage.memory

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

class MemoryDaoTest extends FunSpec with Matchers {
  case class Record(
    id: String,
    data: String
  )

  it("behaves as a dao") {
    val dao = new MemoryDao[Record]()

    dao.get(id = "1") shouldBe Success(None)
    dao.get(id = "2") shouldBe Success(None)

    val r1 = Record(id = "1", data = "hello world")
    dao.put(r1) shouldBe Success(r1)
    dao.get(id = "1") shouldBe Success(Some(r1))

    val r2 = Record(id = "2", data = "howdy friends")
    dao.put(r2) shouldBe Success(r2)
    dao.get(id = "2") shouldBe Success(Some(r2))

    val r1Update = r1.copy(data = "what's up, folks?")
    dao.put(r1Update) shouldBe Success(r1Update)
    dao.get(id = "1") shouldBe Success(Some(r1Update))
  }
}
