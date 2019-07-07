package uk.ac.wellcome.storage.store.dynamo

import java.util.UUID

import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix, Version}
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStore}

class DynamoHybridStore[T, Metadata](prefix: S3ObjectLocationPrefix)(
  implicit
  val indexedStore: DynamoHashStore[
    String,
    Int,
    HybridIndexedStoreEntry[S3ObjectLocation, Metadata]],
  val typedStore: S3TypedStore[T]
) extends HybridStore[Version[String, Int], S3ObjectLocation, T, Metadata] {

  override protected def createTypeStoreId(
    id: Version[String, Int]): S3ObjectLocation =
    prefix.asLocation(id.id, id.version.toString, UUID.randomUUID().toString)
}
