package uk.ac.wellcome.storage.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.fixtures.S3

class S3CopierTest extends FunSpec with Matchers with S3 {

  val s3Copier = new S3Copier(s3Client)

  it("copies an object inside a bucket") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src.txt")
      val dst = createObjectLocationWith(bucket, key = "dst.txt")

      createObject(src)
      listKeysInBucket(bucket) shouldBe List(src.key)

      s3Copier.copy(src = src, dst = dst)

      listKeysInBucket(bucket) should contain theSameElementsAs List(
        src.key,
        dst.key)
      assertEqualObjects(src, dst)
    }
  }

  it("copies a file across different buckets") {
    withLocalS3Bucket { srcBucket =>
      val src = createObjectLocationWith(srcBucket)

      withLocalS3Bucket { dstBucket =>
        val dst = createObjectLocationWith(dstBucket)

        createObject(src)
        listKeysInBucket(srcBucket) shouldBe List(src.key)
        listKeysInBucket(dstBucket) shouldBe List()

        s3Copier.copy(src = src, dst = dst)

        listKeysInBucket(srcBucket) shouldBe List(src.key)
        listKeysInBucket(dstBucket) shouldBe List(dst.key)
        assertEqualObjects(src, dst)
      }
    }
  }

  it("fails if the source object does not exist") {
    val src = createObjectLocation
    val dst = createObjectLocation

    intercept[AmazonS3Exception] {
      s3Copier.copy(src, dst)
    }
  }

  it("fails if the destination bucket does not exist") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket)
      val dst = createObjectLocationWith(Bucket("no_such_bucket"))

      intercept[AmazonS3Exception] {
        s3Copier.copy(src, dst)
      }
    }
  }

  it("copies an object if the destination object already exists and is the same") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src.txt")
      val dst = createObjectLocationWith(bucket, key = "dst.txt")

      createObject(src, content = "hello")
      createObject(dst, content = "hello")

      s3Copier.copy(src = src, dst = dst)

      assertEqualObjects(src, dst)
    }
  }

  it("fails if the destination object exists and is different") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src.txt")
      val dst = createObjectLocationWith(bucket, key = "dst.txt")

      createObject(src, content = "hello")
      createObject(dst, content = "different")

      intercept[RuntimeException] {
        s3Copier.copy(src, dst)
      }
    }
  }
}
