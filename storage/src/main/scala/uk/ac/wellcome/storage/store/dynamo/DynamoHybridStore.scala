package uk.ac.wellcome.storage.store.dynamo

import java.util.UUID

import uk.ac.wellcome.storage.store._
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage._

class DynamoHybridStore[T, Metadata](prefix: ObjectLocationPrefix)(
  implicit val indexedStore: DynamoHashStore[
    String,
    Int,
    HybridIndexedStoreEntry[ObjectLocation, Metadata]],
  val typedStore: S3TypedStore[T]
) extends HybridStore[Version[String, Int], ObjectLocation, T, Metadata] {

  override protected def createTypeStoreId(
    id: Version[String, Int]): ObjectLocation =
    prefix.asLocation(
      id.id,
      id.version.toString,
      UUID.randomUUID().toString + ".json")

  override protected def getTypedStoreEntry(typedStoreId: ObjectLocation)
    : Either[ReadError, Identified[ObjectLocation, T]] =
    super.getTypedStoreEntry(typedStoreId) match {
      case Right(t) => Right(t)
      case Left(err: StoreReadError)
          if err.e.getMessage.startsWith("The specified bucket is not valid") =>
        Left(DanglingHybridStorePointerError(err.e))
      case Left(err) => Left(err)
    }
}
