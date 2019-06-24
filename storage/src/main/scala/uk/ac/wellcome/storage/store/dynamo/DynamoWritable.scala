package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.query._
import org.scanamo.syntax._
import org.scanamo.{DynamoFormat, Scanamo, Table}
import uk.ac.wellcome.storage.{Identified, StoreWriteError, Version}
import uk.ac.wellcome.storage.dynamo.{
  DynamoEntry,
  DynamoHashEntry,
  DynamoHashRangeEntry
}
import uk.ac.wellcome.storage.store.Writable

import scala.util.{Failure, Success, Try}

sealed trait DynamoWritable[Ident, EntryType <: DynamoEntry[_, T], T]
    extends Writable[Ident, T] {

  protected val client: AmazonDynamoDB
  protected val table: Table[EntryType]

  protected def createEntry(id: Ident, t: T): EntryType

  protected def tableGiven(id: Ident): ConditionalOperation[EntryType, _]

  override def put(id: Ident)(t: T): WriteEither = {
    val entry = createEntry(id, t)

    val ops = tableGiven(id).put(entry)

    Try(Scanamo(client).exec(ops)) match {
      case Success(Right(_))  => Right(Identified(id, entry.payload))
      case Success(Left(err)) => Left(StoreWriteError(err))
      case Failure(err)       => Left(StoreWriteError(err))
    }
  }
}

trait DynamoHashWritable[HashKey, V, T]
    extends DynamoWritable[
      Version[HashKey, V],
      DynamoHashEntry[HashKey, V, T],
      T] {
  implicit protected val formatV: DynamoFormat[V]
  assert(formatV != null)

  override protected def createEntry(id: Version[HashKey, V],
                                     t: T): DynamoHashEntry[HashKey, V, T] =
    DynamoHashEntry(id.id, id.version, t)

  override protected def tableGiven(id: Version[HashKey, V])
    : ConditionalOperation[DynamoHashEntry[HashKey, V, T], _] =
    table.given(
      not(attributeExists('hashKey)) or
        (attributeExists('hashKey) and 'version < id.version)
    )
}

trait DynamoHashRangeWritable[HashKey, RangeKey, T]
    extends DynamoWritable[
      Version[HashKey, RangeKey],
      DynamoHashRangeEntry[HashKey, RangeKey, T],
      T] {
  implicit protected val formatRangeKey: DynamoFormat[RangeKey]
  assert(formatRangeKey != null)

  override protected def createEntry(
    id: Version[HashKey, RangeKey],
    t: T): DynamoHashRangeEntry[HashKey, RangeKey, T] =
    DynamoHashRangeEntry(id.id, id.version, t)

  override protected def tableGiven(id: Version[HashKey, RangeKey])
    : ConditionalOperation[DynamoHashRangeEntry[HashKey, RangeKey, T], _] =
    table.given(
      not(attributeExists('hashKey))
    )
}
