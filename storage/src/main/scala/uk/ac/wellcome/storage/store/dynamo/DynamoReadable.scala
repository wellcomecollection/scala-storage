package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import grizzled.slf4j.Logging
import org.scanamo.query.Query
import org.scanamo.syntax._
import org.scanamo.{DynamoFormat, Scanamo, Table}
import uk.ac.wellcome.storage.dynamo.{
  DynamoEntry,
  DynamoHashEntry,
  DynamoHashRangeEntry
}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

sealed trait DynamoReadable[
  Ident, DynamoIdent, EntryType <: DynamoEntry[_, T], T]
    extends Readable[Ident, T] {

  implicit protected val format: DynamoFormat[EntryType]

  protected val client: AmazonDynamoDB
  protected val table: Table[EntryType]

  protected def createKeyExpression(id: DynamoIdent): Query[_]

  protected def getEntry(id: DynamoIdent): Either[ReadError, EntryType] = {
    val ops = table.query(createKeyExpression(id))

    Try(Scanamo(client).exec(ops)) match {
      case Success(List(Right(entry))) => Right(entry)
      case Success(List(Left(err))) =>
        val daoReadError = new Error(s"DynamoReadError: ${err.toString}")

        Left(StoreReadError(daoReadError))
      case Success(Nil) => Left(DoesNotExistError())
      case Failure(err) => Left(StoreReadError(err))
    }
  }
}

trait DynamoHashReadable[HashKey, V, T]
    extends DynamoReadable[
      Version[HashKey, V],
      HashKey,
      DynamoHashEntry[HashKey, V, T],
      T]
    with Logging {
  implicit protected val formatHashKey: DynamoFormat[HashKey]

  protected def createKeyExpression(id: HashKey): Query[_] =
    'hashKey -> id

  override def get(id: Version[HashKey, V]): ReadEither = {
    val storedEntry = getEntry(id.id)
    debug(s"READ: Got Dynamo entry $storedEntry")

    storedEntry.flatMap { entry =>
      if (entry.version == id.version) {
        Right(Identified(id, entry.payload))
      } else {
        Left(NoVersionExistsError())
      }
    }
  }
}

// TODO: Think of a better name than 'Version'
trait DynamoHashRangeReadable[HashKey, RangeKey, T]
    extends DynamoReadable[
      Version[HashKey, RangeKey],
      Version[HashKey, RangeKey],
      DynamoHashRangeEntry[HashKey, RangeKey, T],
      T] {

  implicit val formatHashKey: DynamoFormat[HashKey]
  implicit val formatRangeKey: DynamoFormat[RangeKey]

  protected def createKeyExpression(id: Version[HashKey, RangeKey]): Query[_] =
    'hashKey -> id.id and 'rangeKey -> id.version

  override def get(id: Version[HashKey, RangeKey]): ReadEither =
    getEntry(id).map { entry =>
      Identified(id, entry.payload)
    }
}
