package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.{DynamoFormat, Table}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo.{
  DynamoConfig,
  DynamoHashEntry,
  DynamoHashRangeEntry
}
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.maxima.dynamo.DynamoHashRangeMaxima
import uk.ac.wellcome.storage.store._

class DynamoHashRangeStore[HashKey, RangeKey, T](val config: DynamoConfig)(
  implicit val client: AmazonDynamoDB,
  val formatHashKey: DynamoFormat[HashKey],
  val formatRangeKey: DynamoFormat[RangeKey],
  val format: DynamoFormat[DynamoHashRangeEntry[HashKey, RangeKey, T]]
) extends Store[Version[HashKey, RangeKey], T]
    with DynamoHashRangeReadable[HashKey, RangeKey, T]
    with DynamoHashRangeWritable[HashKey, RangeKey, T]
    with DynamoHashRangeMaxima[
      HashKey,
      RangeKey,
      DynamoHashRangeEntry[HashKey, RangeKey, T]] {

  override protected val table =
    Table[DynamoHashRangeEntry[HashKey, RangeKey, T]](config.tableName)

}

class DynamoHashStore[HashKey, V, T](val config: DynamoConfig)(
  implicit val client: AmazonDynamoDB,
  val formatHashKey: DynamoFormat[HashKey],
  val formatV: DynamoFormat[V],
  val format: DynamoFormat[DynamoHashEntry[HashKey, V, T]]
) extends Store[Version[HashKey, V], T]
    with DynamoHashReadable[HashKey, V, T]
    with DynamoHashWritable[HashKey, V, T]
    with Maxima[HashKey, V] {
  override def max(hashKey: HashKey): Either[ReadError, V] =
    getEntry(hashKey).map { _.version } match {
      case Right(value)               => Right(value)
      case Left(_: DoesNotExistError) => Left(NoMaximaValueError())
      case Left(err)                  => Left(err)
    }

  override protected val table =
    Table[DynamoHashEntry[HashKey, V, T]](config.tableName)
}
