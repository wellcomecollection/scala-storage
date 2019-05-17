package uk.ac.wellcome.storage.memory

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.StorageHelpers

import scala.util.{Failure, Random, Success}

class MemoryConditionalUpdateDaoTest extends FunSpec with Matchers with StorageHelpers {
  case class VersionedRecord(
    id: String,
    version: Int,
    data: String
  )

  it("behaves as a dao") {
    val dao = createConditionalUpdateDao[String, VersionedRecord]

    dao.get(id = "1") shouldBe Success(None)
    dao.get(id = "2") shouldBe Success(None)

    val r1 = VersionedRecord(id = "1", data = "hello world", version = 1)
    dao.put(r1) shouldBe Success(r1)
    dao.get(id = "1") shouldBe Success(Some(r1))

    val r2 = VersionedRecord(id = "2", data = "howdy friends", version = 2)
    dao.put(r2) shouldBe Success(r2)
    dao.get(id = "2") shouldBe Success(Some(r2))
  }

  it("allows updating a record with a newer version") {
    val dao = createConditionalUpdateDao[String, VersionedRecord]

    (1 to 5).map { version =>
      val record = VersionedRecord(
        id = "x",
        data = Random.alphanumeric take 10 mkString,
        version = version
      )

      dao.put(record) shouldBe Success(record)
      dao.get(id = "x") shouldBe Success(Some(record))
    }
  }

  it("blocks updating a record with an older version") {
    val dao = createConditionalUpdateDao[String, VersionedRecord]

    val v5 = VersionedRecord(
      id = "x",
      data = Random.alphanumeric take 10 mkString,
      version = 5
    )

    dao.put(v5)

    val v2 = v5.copy(version = 2)
    val result = dao.put(v2)

    result shouldBe a[Failure[_]]
    val err = result.failed.get
    err shouldBe a[Throwable]
    err.getMessage should startWith("Rejected! Version is going backwards")
  }

  it("blocks updating a record with the same version") {
    val dao = createConditionalUpdateDao[String, VersionedRecord]

    val record = VersionedRecord(
      id = "x",
      data = Random.alphanumeric take 10 mkString,
      version = 5
    )

    dao.put(record)

    val result = dao.put(record)

    result shouldBe a[Failure[_]]
    val err = result.failed.get
    err shouldBe a[Throwable]
    err.getMessage should startWith("Rejected! Version is going backwards")
  }
}
