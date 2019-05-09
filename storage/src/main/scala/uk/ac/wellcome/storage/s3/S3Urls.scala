package uk.ac.wellcome.storage.s3

import java.net.URI

import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

object S3Urls {
  def encode(objectLocation: ObjectLocation): URI =
    new URI(s"s3://${objectLocation.namespace}/${objectLocation.key}")

  def decode(uri: URI): Try[ObjectLocation] = Try {
    if (uri.getScheme != "s3") {
      throw new IllegalArgumentException(
        s"URI does not use the s3:// scheme: $uri"
      )
    }

    ObjectLocation(
      namespace = uri.getHost,
      key = uri.getPath.stripPrefix("/")
    )
  }
}
