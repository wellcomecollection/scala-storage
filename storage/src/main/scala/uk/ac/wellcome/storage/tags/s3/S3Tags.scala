package uk.ac.wellcome.storage.tags.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{
  GetObjectTaggingRequest,
  ObjectTagging,
  SetObjectTaggingRequest,
  Tag
}
import uk.ac.wellcome.storage.s3.S3Get
import uk.ac.wellcome.storage.{
  ObjectLocation,
  ReadError,
  StoreWriteError,
  WriteError
}
import uk.ac.wellcome.storage.tags.Tags

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3Tags(maxRetries: Int = 3)(implicit s3Client: AmazonS3)
    extends Tags[ObjectLocation] {
  override def get(
    location: ObjectLocation): Either[ReadError, Map[String, String]] =
    for {
      response <- S3Get.get(location, maxRetries = maxRetries) {
        location: ObjectLocation =>
          s3Client.getObjectTagging(
            new GetObjectTaggingRequest(location.namespace, location.path)
          )
      }

      tags = response.getTagSet.asScala.map { tag: Tag =>
        tag.getKey -> tag.getValue
      }.toMap
    } yield tags

  override protected def put(
    location: ObjectLocation,
    tags: Map[String, String]): Either[WriteError, Map[String, String]] = {
    val tagSet = tags
      .map { case (k, v) => new Tag(k, v) }
      .toSeq
      .asJava

    Try {
      s3Client.setObjectTagging(
        new SetObjectTaggingRequest(
          location.namespace,
          location.path,
          new ObjectTagging(tagSet)
        )
      )
    } match {
      case Success(_)   => Right(tags)
      case Failure(err) => Left(StoreWriteError(err))
    }
  }
}
