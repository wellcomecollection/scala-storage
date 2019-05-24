package uk.ac.wellcome.storage.memory

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.DoesNotExistError

import scala.util.Random

class MemoryConditionalUpdateDaoTest extends FunSpec with Matchers with EitherValues {
  case class VersionedRecord(
    id: String,
    version: Int,
    data: String
  )

  it("behaves as a dao") {
    val dao = MemoryConditionalUpdateDao[String, VersionedRecord]()

    dao.get(id = "1").left.value shouldBe a[DoesNotExistError]
    dao.get(id = "2").left.value shouldBe a[DoesNotExistError]

    val r1 = VersionedRecord(id = "1", data = "hello world", version = 1)
    dao.put(r1) shouldBe Right(())
    dao.get(id = "1") shouldBe Right(r1)

    val r2 = VersionedRecord(id = "2", data = "howdy friends", version = 2)
    dao.put(r2) shouldBe Right(())
    dao.get(id = "2") shouldBe Right(r2)
  }

  it("allows updating a record with a newer version") {
    val dao = MemoryConditionalUpdateDao[String, VersionedRecord]()

    (1 to 5).map { version =>
      val record = VersionedRecord(
        id = "x",
        data = Random.alphanumeric take 10 mkString,
        version = version
      )

      dao.put(record) shouldBe Right(())
      dao.get(id = "x") shouldBe Right(record)
    }
  }

  it("blocks updating a record with an older version") {
    val dao = MemoryConditionalUpdateDao[String, VersionedRecord]()

    val v5 = VersionedRecord(
      id = "x",
      data = Random.alphanumeric take 10 mkString,
      version = 5
    )

    dao.put(v5)

    val v2 = v5.copy(version = 2)
    val result = dao.put(v2)

    val err = result.left.value.e
    err shouldBe a[Throwable]
    err.getMessage should startWith("Rejected! Version is going backwards")
  }

  it("blocks updating a record with the same version") {
    val dao = MemoryConditionalUpdateDao[String, VersionedRecord]()

    val record = VersionedRecord(
      id = "x",
      data = Random.alphanumeric take 10 mkString,
      version = 5
    )

    dao.put(record)

    val result = dao.put(record)

    val err = result.left.value.e
    err shouldBe a[Throwable]
    err.getMessage should startWith("Rejected! Version is going backwards")
  }
}
