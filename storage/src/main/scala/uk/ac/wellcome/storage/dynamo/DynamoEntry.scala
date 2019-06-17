package uk.ac.wellcome.storage.dynamo

sealed trait DynamoEntry[HashKey, T] {
  val hashKey: HashKey
  val payload: T
}

trait DynamoHashRangeKeyPair[HashKey, RangeKey] {
  val hashKey: HashKey
  val rangeKey: RangeKey
}

case class DynamoHashRangeEntry[HashKey, RangeKey, T](
  hashKey: HashKey,
  rangeKey: RangeKey,
  payload: T
) extends DynamoEntry[HashKey, T]
    with DynamoHashRangeKeyPair[HashKey, RangeKey]

case class DynamoHashEntry[HashKey, V, T](
  hashKey: HashKey,
  version: V,
  payload: T
) extends DynamoEntry[HashKey, T]
