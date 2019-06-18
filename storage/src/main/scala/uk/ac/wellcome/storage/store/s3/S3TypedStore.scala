package uk.ac.wellcome.storage.store.s3

import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.streaming.Codec

class S3TypedStore[T](
  implicit
  val codec: Codec[T],
  val streamStore: S3StreamStore
) extends TypedStore[ObjectLocation, T]

object S3TypedStore {
  def apply[T](implicit codec: Codec[T], streamStore: S3StreamStore): S3TypedStore[T] =
    new S3TypedStore[T]()(codec, streamStore)
}
