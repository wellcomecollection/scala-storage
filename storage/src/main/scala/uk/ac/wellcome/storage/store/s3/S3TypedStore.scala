package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.streaming.Codec

class S3TypedStore[T](
  implicit
  val codec: Codec[T],
  val streamStore: S3StreamStore
) extends TypedStore[S3ObjectLocation, T]

object S3TypedStore {
  def apply[T](implicit
               codec: Codec[T],
               s3Client: AmazonS3): S3TypedStore[T] = {
    implicit val streamStore: S3StreamStore = new S3StreamStore()

    new S3TypedStore[T]()
  }
}
