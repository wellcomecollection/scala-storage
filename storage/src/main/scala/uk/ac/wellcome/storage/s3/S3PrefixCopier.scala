package uk.ac.wellcome.storage.s3

import uk.ac.wellcome.storage.{ObjectCopier, ObjectLocation}

import scala.concurrent.Future

class S3PrefixCopier(s3PrefixOperator: S3PrefixOperator, copier: ObjectCopier) {

  /** Copy all the objects from under ObjectLocation into another ObjectLocation,
    * preserving the relative path from the source in the destination.
    *
    * e.g. if you copy s3://bucket/foo to s3://other_bucket/bar, this
    * function would copy
    *
    *     s3://bucket/foo/0.txt       ~> s3://other_bucket/bar/0.txt
    *     s3://bucket/foo/1.txt       ~> s3://other_bucket/bar/1.txt
    *     s3://bucket/foo/2/20.txt    ~> s3://other_bucket/bar/2/20.txt
    *     s3://bucket/foo/2/21.txt    ~> s3://other_bucket/bar/2/21.txt
    *
    */
  def copyObjects(
    srcLocationPrefix: ObjectLocation,
    dstLocationPrefix: ObjectLocation
  ): Future[Unit] =
    s3PrefixOperator.run(prefix = srcLocationPrefix) { srcLocation: ObjectLocation =>
      val relativeKey = srcLocation.key
        .stripPrefix(srcLocationPrefix.key)

      val dstLocation = ObjectLocation(
        namespace = dstLocationPrefix.namespace,
        key = dstLocationPrefix.key + relativeKey
      )

      copier.copy(srcLocation, dstLocation)
    }
}
