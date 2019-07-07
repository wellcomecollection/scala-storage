package uk.ac.wellcome.storage.store.dynamo

import java.util.UUID

import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.store.{
  HybridIndexedStoreEntry,
  HybridStoreWithMaxima
}

class DynamoHybridStoreWithMaxima[Id, V, T, Metadata](
  prefix: S3ObjectLocationPrefix)(
  implicit
  val indexedStore: DynamoHashRangeStore[
    Id,
    V,
    HybridIndexedStoreEntry[S3ObjectLocation, Metadata]],
  val typedStore: S3TypedStore[T]
) extends HybridStoreWithMaxima[Id, V, S3ObjectLocation, T, Metadata] {

  override protected def createTypeStoreId(
    id: Version[Id, V]): S3ObjectLocation =
    prefix.asLocation(
      id.id.toString,
      id.version.toString,
      UUID.randomUUID().toString)
}
