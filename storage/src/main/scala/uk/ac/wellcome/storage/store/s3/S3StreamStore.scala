package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming._

class S3StreamStore(val maxRetries: Int = 2)(implicit val s3Client: AmazonS3)
    extends StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata]
    with S3StreamReadable
    with S3StreamWritable
