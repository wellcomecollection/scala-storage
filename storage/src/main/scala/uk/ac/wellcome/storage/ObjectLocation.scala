package uk.ac.wellcome.storage

import java.nio.file.Paths

trait BetterObjectLocation {
  val namespace: String
  val path: String

  override def toString = s"$namespace/$path"

  def join(parts: String*): _ <: BetterObjectLocation

  def asPrefix: _ <: BetterObjectLocationPrefix
}

trait BetterObjectLocationPrefix {
  val namespace: String
  val pathPrefix: String

  def asLocation(parts: String*): _ <: BetterObjectLocation
}

case class S3ObjectLocation(bucket: String, key: String) extends BetterObjectLocation {
  val namespace: String = bucket
  val path: String = key

  override def toString = s"s3://$bucket/$key"

  override def join(parts: String*): S3ObjectLocation =
    this.copy(
      key = Paths.get(this.key, parts: _*).toString
    )

  override def asPrefix: S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = bucket,
      keyPrefix = key
    )
}

case class S3ObjectLocationPrefix(bucket: String, keyPrefix: String) extends BetterObjectLocationPrefix {
  override val namespace: String = bucket
  override val pathPrefix: String = keyPrefix

  override def asLocation(parts: String*): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket,
      key = Paths.get(this.keyPrefix, parts: _*).toString
    )
}
