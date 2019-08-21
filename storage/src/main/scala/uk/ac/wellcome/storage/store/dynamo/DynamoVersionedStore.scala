package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.DynamoFormat
import uk.ac.wellcome.storage.dynamo.{
  DynamoConfig,
  DynamoHashEntry,
  DynamoHashRangeEntry
}
import uk.ac.wellcome.storage.store.VersionedStore

class DynamoMultipleVersionStore[Id, T](val config: DynamoConfig)(
  implicit val client: AmazonDynamoDB,
  val formatHashKey: DynamoFormat[Id],
  val formatRangeKey: DynamoFormat[Int],
  val formatT: DynamoFormat[T],
  val formatDynamoHashRangeEntry: DynamoFormat[DynamoHashRangeEntry[Id, Int, T]]
) extends VersionedStore[Id, Int, T](
      new DynamoHashRangeStore[Id, Int, T](config)
    )

class DynamoSingleVersionStore[Id, T](val config: DynamoConfig)(
  implicit val client: AmazonDynamoDB,
  val formatHashKey: DynamoFormat[Id],
  val formatRangeKey: DynamoFormat[Int],
  val formatT: DynamoFormat[T],
  val formatDynamoHashEntry: DynamoFormat[DynamoHashEntry[Id, Int, T]]
) extends VersionedStore[Id, Int, T](
      new DynamoHashStore[Id, Int, T](config)
    )
