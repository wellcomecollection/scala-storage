package uk.ac.wellcome.storage.dynamo

import java.time.Instant

import com.gu.scanamo.{DynamoFormat, Scanamo}
import com.gu.scanamo.syntax._
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDbVersioned

class DynamoFormatTest extends FunSpec with LocalDynamoDbVersioned with Matchers {
  trait Identifiable {
    val id: String
  }

  it("allows storing and retrieving an instance of an Instant") {
    case class Timestamp(id: String, datetime: Instant) extends Identifiable

    val record = Timestamp(id = "1", datetime = Instant.now)

    assertCanStoreAndRetrieve[Timestamp](record = record)
  }

  private def assertCanStoreAndRetrieve[T <: Identifiable](record: T)(
    implicit format: DynamoFormat[T]): Assertion =
    withLocalDynamoDbTable { table =>
      Scanamo.put(dynamoDbClient)(table.name)(record)
      Scanamo
        .get[T](dynamoDbClient)(table.name)('id -> record.id)
        .get shouldBe Right(record)
    }
}
