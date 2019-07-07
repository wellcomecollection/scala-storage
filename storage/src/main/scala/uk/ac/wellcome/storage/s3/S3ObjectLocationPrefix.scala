package uk.ac.wellcome.storage.s3

import java.nio.file.Paths

import uk.ac.wellcome.storage.NamespacedPath

case class S3ObjectLocationPrefix(bucket: String, keyPrefix: String) extends NamespacedPath {
  override val namespace: String = bucket
  override val path: String = keyPrefix

  def asLocation(parts: String*): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket,
      key = Paths.get(this.keyPrefix, parts: _*).toString
    )
}
