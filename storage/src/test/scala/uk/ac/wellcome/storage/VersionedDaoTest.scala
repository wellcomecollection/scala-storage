package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.StorageHelpers

import scala.util.{Failure, Success}

class VersionedDaoTest extends FunSpec with Matchers with StorageHelpers {
  case class VersionedRecord(
    id: String,
    version: Int,
    data: String
  )

  it("behaves correctly") {
    val dao = createVersionedDao[String, VersionedRecord]

    // Check it increments the record upon storing
    val record1 = VersionedRecord(id = "1", version = 0, data = "first")
    val record2 = VersionedRecord(id = "2", version = 0, data = "second")

    dao.put(record1) shouldBe Success(record1.copy(version = 1))
    dao.put(record2) shouldBe Success(record2.copy(version = 1))

    dao.get(record1.id) shouldBe Success(Some(record1.copy(version = 1)))
    dao.get(record2.id) shouldBe Success(Some(record2.copy(version = 1)))

    dao.get("doesnotexist") shouldBe Success(None)

    dao.put(record1) shouldBe a[Failure[_]]
    dao.put(record2) shouldBe a[Failure[_]]

    // Put with a higher version
    dao.put(record1.copy(version = 5)) shouldBe Success(record1.copy(version = 6))

    // Put with the same version
    dao.put(record1.copy(version = 6)) shouldBe Success(record1.copy(version = 7))

    // Put with a lower version
    dao.put(record1.copy(version = 4)) shouldBe a[Failure[_]]
  }
}
