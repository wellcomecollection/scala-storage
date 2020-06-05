package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.StreamStore

class S3StreamStore(val maxRetries: Int = 2)(implicit val s3Client: AmazonS3)
    extends StreamStore[ObjectLocation]
    with S3StreamReadable
    with S3StreamWritable
