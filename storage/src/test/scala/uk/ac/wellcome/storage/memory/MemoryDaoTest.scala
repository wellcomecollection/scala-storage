package uk.ac.wellcome.storage.memory

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.DoesNotExistError

class MemoryDaoTest extends FunSpec with Matchers with EitherValues {
  case class Record(
    id: String,
    data: String
  )

  it("behaves as a dao") {
    val dao = new MemoryDao[String, Record]()

    dao.get(id = "1").left.value shouldBe a[DoesNotExistError]
    dao.get(id = "2").left.value shouldBe a[DoesNotExistError]

    val r1 = Record(id = "1", data = "hello world")
    dao.put(r1) shouldBe Right(())
    dao.get(id = "1") shouldBe Right(r1)

    val r2 = Record(id = "2", data = "howdy friends")
    dao.put(r2) shouldBe Right(())
    dao.get(id = "2") shouldBe Right(r2)

    val r1Update = r1.copy(data = "what's up, folks?")
    dao.put(r1Update) shouldBe Right(())
    dao.get(id = "1") shouldBe Right(r1Update)
  }
}
