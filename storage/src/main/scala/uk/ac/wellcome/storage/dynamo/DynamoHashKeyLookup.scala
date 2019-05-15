package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item => DynamoItem}
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.{DynamoFormat, ScanamoFree}

import scala.util.Try

case class DynamoHashKeyLookupConfig(
  hashKeyName: String,
  dynamoConfig: DynamoConfig
)

/** If you have a DynamoDB table with a hash key and a range key, and you
  * want to look up record with the highest/lowest value of the range key
  * associated with this hash key, you can use lookupHighestHashKey and
  * lookupLowestHashKey.
  *
  * e.g. if your table schema is
  *
  *     HASH    identifier
  *     RANGE   version
  *
  * with one row per (identifier, version) pair, and you want to look up
  * the highest/lowest version associated with each identifier.
  *
  */
class DynamoHashKeyLookup[T, HashKeyValue](
  dynamoClient: AmazonDynamoDB,
  lookupConfig: DynamoHashKeyLookupConfig
)(implicit
  evidence: DynamoFormat[T]) {

  private val documentClient = new DynamoDB(dynamoClient)

  private def lookup(value: HashKeyValue,
                     lowestValueFirst: Boolean): Try[Option[T]] = Try {

    // Query results are sorted by the range key.
    val querySpec = new QuerySpec()
      .withHashKey(lookupConfig.hashKeyName, value)
      .withConsistentRead(true)
      .withScanIndexForward(lowestValueFirst)
      .withMaxResultSize(1)

    val result = documentClient
      .getTable(lookupConfig.dynamoConfig.table)
      .query(querySpec)
      .iterator()

    if (result.hasNext) {
      val item: DynamoItem = result.next()
      val avMap: java.util.Map[String, AttributeValue] =
        InternalUtils.toAttributeValues(item)
      ScanamoFree.read[T](avMap) match {
        case Right(record) => Some(record)
        case Left(error) =>
          throw new RuntimeException(
            s"Error parsing $item with Scanamo: $error")
      }
    } else {
      None
    }
  }

  def lookupHighestHashKey(value: HashKeyValue): Try[Option[T]] =
    lookup(value, lowestValueFirst = false)

  def lookupLowestHashKey(value: HashKeyValue): Try[Option[T]] =
    lookup(value, lowestValueFirst = true)
}
