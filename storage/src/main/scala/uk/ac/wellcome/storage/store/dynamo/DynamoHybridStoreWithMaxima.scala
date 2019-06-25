package uk.ac.wellcome.storage.store.dynamo

import java.util.UUID

import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix, Version}
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreWithMaxima}
import uk.ac.wellcome.storage.store.s3.S3TypedStore

class DynamoHybridStoreWithMaxima[T, Metadata](prefix: ObjectLocationPrefix)(
  implicit
  val indexedStore: DynamoHashRangeStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Metadata]],
  val typedStore: S3TypedStore[T]
) extends HybridStoreWithMaxima[String, Int, ObjectLocation, T, Metadata] {

  override protected def createTypeStoreId(id: Version[String, Int]): ObjectLocation =
    prefix.asLocation(id.id, id.version.toString, UUID.randomUUID().toString)
}