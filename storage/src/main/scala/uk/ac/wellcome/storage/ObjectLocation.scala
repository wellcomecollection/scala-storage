package uk.ac.wellcome.storage

import java.nio.file.Paths

trait NamespacedPath {
  val namespace: String
  val path: String

  override def toString = s"$namespace/$path"

  protected def joinPaths(parts: String*): String =
    Paths.get(path, parts: _*).toString
}

case class S3ObjectLocation(bucket: String, key: String) extends NamespacedPath {
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

case class S3ObjectLocationPrefix(bucket: String, keyPrefix: String) extends NamespacedPath {
  override val namespace: String = bucket
  override val path: String = keyPrefix

  def asLocation(parts: String*): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket,
      key = Paths.get(this.keyPrefix, parts: _*).toString
    )
}
