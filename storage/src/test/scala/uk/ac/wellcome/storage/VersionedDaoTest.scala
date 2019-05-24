package uk.ac.wellcome.storage

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.MemoryBuilders

class VersionedDaoTest extends FunSpec with Matchers with EitherValues with MemoryBuilders {
  case class VersionedRecord(
    id: String,
    version: Int,
    data: String
  )

  it("behaves correctly") {
    val dao = createVersionedDao[VersionedRecord]

    // Check it increments the record upon storing
    val record1 = VersionedRecord(id = "1", version = 0, data = "first")
    val record2 = VersionedRecord(id = "2", version = 0, data = "second")

    dao.put(record1) shouldBe Right(record1.copy(version = 1))
    dao.put(record2) shouldBe Right(record2.copy(version = 1))

    dao.get(record1.id) shouldBe Right(record1.copy(version = 1))
    dao.get(record2.id) shouldBe Right(record2.copy(version = 1))

    dao.get("doesnotexist").left.value shouldBe a[DoesNotExistError]

    dao.put(record1).left.value shouldBe a[ConditionalWriteError]
    dao.put(record2).left.value shouldBe a[ConditionalWriteError]

    // Put with a higher version
    dao.put(record1.copy(version = 5)) shouldBe Right(record1.copy(version = 6))

    // Put with the same version
    dao.put(record1.copy(version = 6)) shouldBe Right(record1.copy(version = 7))

    // Put with a lower version
    dao.put(record1.copy(version = 4)).left.value shouldBe a[ConditionalWriteError]
  }
}
