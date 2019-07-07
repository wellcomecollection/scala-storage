package uk.ac.wellcome.storage.s3

import uk.ac.wellcome.storage.NamespacedPath

case class S3ObjectLocation(bucket: String, key: String)
    extends NamespacedPath {
  val namespace: String = bucket
  val path: String = key

  override def toString = s"s3://$bucket/$key"

  def join(parts: String*): S3ObjectLocation =
    this.copy(
      key = joinPaths(parts: _*)
    )

  def asPrefix: S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = bucket,
      keyPrefix = key
    )
}
