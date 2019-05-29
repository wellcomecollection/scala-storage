package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.DoesNotExistError
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.util.Random

class DynamoConditionalUpdateDaoTest extends FunSpec with Matchers with EitherValues with LocalDynamoDb {
  case class VersionedRecord(
    id: String,
    data: String,
    version: Int
  )

  def createTable(table: Table): Table =
    createTableWithHashKey(table, keyName = "id")

  it("behaves as a dao") {
    withLocalDynamoDbTable { table =>
      withDynamoConditionalUpdateDao[VersionedRecord, Assertion](table) { dao =>
        dao.get(id = "1").left.value shouldBe a[DoesNotExistError]
        dao.get(id = "2").left.value shouldBe a[DoesNotExistError]

        val r1 = VersionedRecord(id = "1", data = "hello world", version = 1)
        dao.put(r1) shouldBe Right(())
        dao.get(id = "1") shouldBe Right(r1)

        val r2 = VersionedRecord(id = "2", data = "howdy friends", version = 2)
        dao.put(r2) shouldBe Right(())
        dao.get(id = "2") shouldBe Right(r2)
      }
    }
  }

  it("allows updating a record with a newer version") {
    withLocalDynamoDbTable { table =>
      withDynamoConditionalUpdateDao[VersionedRecord, Seq[Assertion]](table) { dao =>
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
    }
  }

  it("blocks updating a record with an older version") {
    withLocalDynamoDbTable { table =>
      withDynamoConditionalUpdateDao[VersionedRecord, Assertion](table) { dao =>
        val v5 = VersionedRecord(
          id = "x",
          data = Random.alphanumeric take 10 mkString,
          version = 5
        )

        dao.put(v5)

        val v2 = v5.copy(version = 2)
        val result = dao.put(v2)

        val err = result.left.value.e
        err shouldBe a[ConditionalCheckFailedException]
        err.getMessage should startWith("The conditional request failed")
      }
    }
  }

  it("blocks updating a record with the same version") {
    withLocalDynamoDbTable { table =>
      withDynamoConditionalUpdateDao[VersionedRecord, Assertion](table) { dao =>
        val record = VersionedRecord(
          id = "x",
          data = Random.alphanumeric take 10 mkString,
          version = 5
        )

        dao.put(record)

        val result = dao.put(record)

        val err = result.left.value.e
        err shouldBe a[ConditionalCheckFailedException]
        err.getMessage should startWith("The conditional request failed")
      }
    }
  }
}
