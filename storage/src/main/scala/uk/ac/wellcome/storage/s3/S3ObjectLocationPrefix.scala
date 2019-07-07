package uk.ac.wellcome.storage.s3

import uk.ac.wellcome.storage.NamespacedPath

case class S3ObjectLocationPrefix(bucket: String, keyPrefix: String)
    extends NamespacedPath {
  override val namespace: String = bucket
  override val path: String = keyPrefix

  override def toString = s"s3://$bucket/$keyPrefix"

  def asLocation(parts: String*): S3ObjectLocation =
    S3ObjectLocation(bucket, key = keyPrefix).join(parts: _*)
}
