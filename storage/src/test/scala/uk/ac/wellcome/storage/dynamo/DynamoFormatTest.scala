package uk.ac.wellcome.storage.dynamo

import java.net.URI
import java.time.Instant
import java.util.UUID

import org.scalatest.{Assertion, FunSpec, Matchers}
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import org.scanamo.DynamoFormat
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

    assertCanStoreAndRetrieve[Timestamp](t = record)
  }

  it("allows storing and retrieving an instance of URI") {
    case class Webpage(id: String, uri: URI) extends Identifiable

    val record = Webpage(id = "1", uri = new URI("https://example.org/"))

    assertCanStoreAndRetrieve[Webpage](t = record)
  }

  it("allows storing and retrieving an instance of UUID") {
    case class UniqueID(id: String, uuid: UUID) extends Identifiable

    val record = UniqueID(id = "1", uuid = UUID.randomUUID())

    assertCanStoreAndRetrieve[UniqueID](t = record)
  }

  private def assertCanStoreAndRetrieve[T <: Identifiable](t: T)(
    implicit format: DynamoFormat[T]): Assertion =
    withLocalDynamoDbTable { table =>
      putTableItem[T](t, table)
      getExistingTableItem[T](id = t.id, table) shouldBe t
    }
}
