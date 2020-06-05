package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.S3Get
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming._

trait S3StreamReadable
    extends Readable[ObjectLocation, InputStreamWithLength]
    with Logging {
  implicit val s3Client: AmazonS3
  val maxRetries: Int

  override def get(location: ObjectLocation): ReadEither =
    S3Get
      .get(location, maxRetries = maxRetries) { location: ObjectLocation =>
        s3Client.getObject(location.namespace, location.path)
      }
      .map { retrievedObject: S3Object =>
        Identified(
          location,
          new InputStreamWithLength(
            retrievedObject.getObjectContent,
            length = retrievedObject.getObjectMetadata.getContentLength
          )
        )
      }
}
