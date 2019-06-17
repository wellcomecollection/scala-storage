package uk.ac.wellcome.storage.dynamo

import java.net.URI
import java.time.Instant
import java.util.UUID

import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo}
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

class DynamoFormatTest extends FunSpec with Matchers with DynamoFixtures {
  trait Identifiable {
    val id: String
  }

  def createTable(table: Table): Table =
    createTableWithHashKey(table, keyName = "id")

  it("allows storing and retrieving an instance of Instant") {
    case class Timestamp(id: String, datetime: Instant) extends Identifiable

    val record = Timestamp(id = "1", datetime = Instant.now)

    assertCanStoreAndRetrieve[Timestamp](record = record)
  }

  it("allows storing and retrieving an instance of URI") {
    case class Webpage(id: String, uri: URI) extends Identifiable

    val record = Webpage(id = "1", uri = new URI("https://example.org/"))

    assertCanStoreAndRetrieve[Webpage](record = record)
  }

  it("allows storing and retrieving an instance of UUID") {
    case class UniqueID(id: String, uuid: UUID) extends Identifiable

    val record = UniqueID(id = "1", uuid = UUID.randomUUID())

    assertCanStoreAndRetrieve[UniqueID](record = record)
  }

  private def assertCanStoreAndRetrieve[T <: Identifiable](record: T)(
    implicit format: DynamoFormat[T]): Assertion =
    withLocalDynamoDbTable { table =>
      Scanamo.put(dynamoClient)(table.name)(record)
      Scanamo
        .get[T](dynamoClient)(table.name)('id -> record.id)
        .get shouldBe Right(record)
    }
}
