package uk.ac.wellcome.storage.s3

import java.nio.file.Paths

import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model._
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.{ObjectCopier, ObjectLocation}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class S3PrefixCopierTest
  extends FunSpec
    with Matchers
    with S3 {

  val batchSize = 10

  val s3PrefixCopier = S3PrefixCopier(
    s3Client = s3Client,
    batchSize = batchSize
  )

  it("succeeds if there are no files in the prefix") {
    withLocalS3Bucket { bucket =>
      val src = createObjectLocationWith(bucket, key = "src/")
      val dst = createObjectLocationWith(bucket, key = "dst/")

      s3PrefixCopier.copyObjects(src, dst) shouldBe a[Success[_]]
      listKeysInBucket(bucket) shouldBe empty
    }
  }

  describe("copying a single file under a prefix") {
    describe("to a key ending in /") {
      it("copies that file") {

        withLocalS3Bucket { dstBucket =>
          withLocalS3Bucket { srcBucket =>

            val srcPrefix = createObjectLocationWith(srcBucket, key = "src")
            val src = srcPrefix.copy(key = Paths.get(srcPrefix.key,"foo.txt").toString)

            createObject(src)

            val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")
            val dst = dstPrefix.copy(key = Paths.get(dstPrefix.key,"foo.txt").toString)

            val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)
            result shouldBe a[Success[_]]

            listKeysInBucket(dstBucket) shouldBe List("dst/foo.txt")
            assertEqualObjects(src, dst)

            result.get.fileCount shouldBe 1
          }
        }
      }
    }

    describe("to a key NOT ending in /") {
      it("copies that file") {

        withLocalS3Bucket { dstBucket =>
          withLocalS3Bucket { srcBucket =>

            val srcPrefix = createObjectLocationWith(srcBucket, key = "src")
            val src = srcPrefix.copy(key = Paths.get(srcPrefix.key,"foo.txt").toString)

            createObject(src)

            val dstPrefix = createObjectLocationWith(dstBucket, key = "dst")
            val dst = dstPrefix.copy(key = Paths.get(dstPrefix.key,"foo.txt").toString)

            val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)
            result shouldBe a[Success[_]]

            listKeysInBucket(dstBucket) shouldBe List("dst/foo.txt")
            assertEqualObjects(src, dst)

            result.get.fileCount shouldBe 1
          }
        }
      }
    }
  }

  it("copies multiple files under a prefix") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        val srcLocations = (1 to 5).map { i =>
          val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
          createObject(src)
          src
        }

        val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")

        val dstLocations = srcLocations.map { loc: ObjectLocation =>
          loc.copy(
            namespace = dstPrefix.namespace,
            key = loc.key.replace("src/", "dst/")
          )
        }

        val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)
        result shouldBe a[Success[_]]

        listKeysInBucket(dstBucket) shouldBe dstLocations.map {
          _.key
        }

        srcLocations.zip(dstLocations).map {
          case (src, dst) =>
            assertEqualObjects(src, dst)
        }

        result.get.fileCount shouldBe 5
      }
    }
  }

  it("fails if the source bucket does not exist") {
    val srcPrefix = createObjectLocation
    val dstPrefix = createObjectLocation

    val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

    result shouldBe a[Failure[_]]
    result.failed.get shouldBe a[AmazonS3Exception]
  }

  it("fails if the destination bucket does not exist") {
    withLocalS3Bucket { bucket =>
      val srcPrefix = createObjectLocationWith(bucket)

      val src = srcPrefix.copy(key = srcPrefix.key + "/foo.txt")
      createObject(src)

      val dstPrefix = createObjectLocationWith(Bucket("no_such_bucket"))

      val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)

      result shouldBe a[Failure[_]]
      result.failed.get shouldBe a[AmazonS3Exception]
    }
  }

  it("copies more objects than are returned in a single ListObject call") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

        // You can get up to 1000 objects in a single S3 ListObject call.
        val count = 10
        val srcLocations = (1 to count).par.map { i =>
          val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
          createObject(src)
          src
        }

        // Check the listKeys call can really retrieve all the objects
        // we're about to copy around!
        listKeysInBucket(srcBucket) should have size count

        val dstPrefix = createObjectLocationWith(dstBucket, key = "dst/")

        val dstLocations = srcLocations.map { loc: ObjectLocation =>
          loc.copy(
            namespace = dstPrefix.namespace,
            key = loc.key.replace("src/", "dst/")
          )
        }

        val result = s3PrefixCopier.copyObjects(srcPrefix, dstPrefix)
        result shouldBe a[Success[_]]

        val actualKeys = listKeysInBucket(dstBucket)
        val expectedKeys = dstLocations.map {
          _.key
        }
        actualKeys.size shouldBe expectedKeys.size
        actualKeys should contain theSameElementsAs expectedKeys

        srcLocations.zip(dstLocations).map {
          case (src, dst) =>
            assertEqualObjects(src, dst)
        }

        result.get.fileCount shouldBe 10
      }
    }
  }

  it("fails if one of the objects fails to copy") {
    val exception = new RuntimeException("Nope, that's not going to work")

    val brokenCopier = new ObjectCopier {
      def copy(src: ObjectLocation, dst: ObjectLocation): Unit =
        if (src.key.endsWith("5.txt"))
          throw exception
    }

    val brokenPrefixCopier = new S3PrefixCopier(
      s3PrefixOperator = new S3PrefixOperator(s3Client),
      copier = brokenCopier
    )

    withLocalS3Bucket { srcBucket =>
      val srcPrefix = createObjectLocationWith(srcBucket, key = "src/")

      (1 to 10).par.map { i =>
        val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
        createObject(src)
        src
      }

      val result = brokenPrefixCopier.copyObjects(
        srcLocationPrefix = srcPrefix,
        dstLocationPrefix = srcPrefix.copy(key = "dst/")
      )

      result shouldBe a[Failure[_]]
      result.failed.get shouldBe exception
    }
  }

  // A modified version of listKeysInBucket that can retrieve everything,
  // even if it takes multiple ListObject calls.
  override def listKeysInBucket(bucket: Bucket): List[String]
  =
    S3Objects
      .inBucket(s3Client, bucket.name)
      .withBatchSize(1000)
      .iterator()
      .asScala
      .toList
      .par
      .map { objectSummary: S3ObjectSummary =>
        objectSummary.getKey
      }
      .toList

}
