package uk.ac.wellcome.storage.dynamo

import java.net.URI
import java.time.Instant
import java.util.UUID

import org.scanamo.syntax._
import org.scanamo.DynamoFormat
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import org.scanamo.auto._
import org.scanamo.error.InvalidPropertiesError
import org.scanamo.{Table => ScanamoTable}
import org.scanamo.time.JavaTimeFormats._

trait DynamoFormatTestCases[T]
    extends FunSpec
    with Matchers
    with DynamoFixtures
    with EitherValues {
  def createTable(table: Table): Table =
    createTableWithHashKey(table)

  def createT: T
  def createBadT: String

  implicit val dynamoFormat: DynamoFormat[T]

  case class RecordT(id: String, t: T)

  it("allows storing and retrieving an instance of T") {
    val record = RecordT(id = "1", t = createT)

    withLocalDynamoDbTable { table =>
      val scanamoTable = ScanamoTable[RecordT](table.name)

      scanamo.exec(scanamoTable.put(record))
      scanamo
        .exec(scanamoTable.get('id -> record.id))
        .get
        .right
        .value shouldBe record
    }
  }

  it("errors if the field is the wrong type") {
    case class BadRecord(id: String, t: String)

    val record = BadRecord(id = "1", t = createBadT)

    withLocalDynamoDbTable { table =>
      scanamo.exec(ScanamoTable[BadRecord](table.name).put(record))

      val scanamoTable = ScanamoTable[RecordT](table.name)
      val err = scanamo.exec(scanamoTable.get('id -> record.id)).get.left.value
      err shouldBe a[InvalidPropertiesError]
    }
  }
}

class DynamoInstantFormatTest extends DynamoFormatTestCases[Instant] {
  def createT: Instant = Instant.now()

  def createBadT: String = "not a valid datetime"

  implicit val dynamoFormat: DynamoFormat[Instant] = instantAsLongFormat
}

class DynamoURIFormatTest extends DynamoFormatTestCases[URI] {
  def createT: URI = new URI("https://example.org")

  def createBadT: String = "not a valid URI"

  implicit val dynamoFormat: DynamoFormat[URI] = uriDynamoFormat
}

class DynamoUUIDFormatTest extends DynamoFormatTestCases[UUID] {
  def createT: UUID = UUID.randomUUID()

  def createBadT: String = "not a valid UUID"

  implicit val dynamoFormat: DynamoFormat[UUID] = DynamoFormat.uuidFormat
}
