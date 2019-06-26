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
  id: HashKey,
  version: RangeKey,
  payload: T
) extends DynamoEntry[HashKey, T]
    with DynamoHashRangeKeyPair[HashKey, RangeKey] {
  val hashKey: HashKey = id
  val rangeKey: RangeKey = version
}

case class DynamoHashEntry[HashKey, V, T](
  id: HashKey,
  version: V,
  payload: T
) extends DynamoEntry[HashKey, T] {
  val hashKey: HashKey = id
}
