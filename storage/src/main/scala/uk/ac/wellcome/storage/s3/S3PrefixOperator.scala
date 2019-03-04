package uk.ac.wellcome.storage.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/** Given an S3 ObjectLocation prefix and a function (ObjectLocation => Unit),
  * apply that function to every object under the prefix.
  *
  */
class S3PrefixOperator(
  s3Client: AmazonS3,
  batchSize: Int = 1000
)(implicit ec: ExecutionContext) {
  def run(prefix: ObjectLocation)(f: ObjectLocation => Unit): Future[Unit] =
    Future {

      // Implementation note: this means we're single-threaded.  We're working
      // on a single object under a prefix at a time.
      //
      // We could rewrite this code to process objects in parallel, but it would
      // make it more complicated and introduces more failure modes.  For now we'll
      // just use this simple version, and we can revisit it if it's not fast
      // enough in practice.

      val objects: Iterator[S3ObjectSummary] = S3Objects
        .withPrefix(
          s3Client,
          prefix.namespace,
          prefix.key
        )
        .withBatchSize(batchSize)
        .iterator()
        .asScala

      val locations = objects.map { summary: S3ObjectSummary =>
        ObjectLocation(
          namespace = prefix.namespace,
          key = summary.getKey
        )
      }

      locations.foreach { loc =>
        f(loc)
      }
    }
}
