package uk.ac.wellcome.storage.memory

import org.scalatest.{FunSpec, Matchers}

import scala.util.{Failure, Success}

class MemoryVersionedDaoTest extends FunSpec with Matchers {
  case class Record(
    id: String,
    version: Int,
    data: String
  )

  it("behaves correctly") {
    val dao = new MemoryVersionedDao[Record]()

    val record1 = Record(id = "1", version = 0, data = "first")
    val record2 = Record(id = "2", version = 0, data = "second")

    dao.put(record1) shouldBe Success(record1.copy(version = 1))
    dao.put(record2) shouldBe Success(record2.copy(version = 1))

    dao.get(record1.id) shouldBe Success(Some(record1.copy(version = 1)))
    dao.get(record2.id) shouldBe Success(Some(record2.copy(version = 1)))

    dao.get("doesnotexist") shouldBe Success(None)

    dao.put(record1) shouldBe a[Failure[_]]
    dao.put(record2) shouldBe a[Failure[_]]

    // Put with a higher version
    dao.put(record1.copy(version = 5)) shouldBe Success(record1.copy(version = 6))

    // Put with a lower version
    dao.put(record1.copy(version = 4)) shouldBe a[Failure[_]]
  }
}
