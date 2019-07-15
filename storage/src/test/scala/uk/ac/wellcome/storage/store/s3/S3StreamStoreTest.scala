package uk.ac.wellcome.storage.store.s3

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage._

class S3StreamStoreTest extends StreamStoreTestCases[ObjectLocation, Bucket, S3StreamStore, Unit] with S3StreamStoreFixtures with BucketNamespaceFixtures {
  describe("handles errors from S3") {
    describe("get") {
      it("errors if S3 has a problem") {
        val store = new S3StreamStore()(brokenS3Client)

        val result = store.get(createObjectLocation).left.value
        result shouldBe a[StoreReadError]

        val err = result.e
        err shouldBe a[SdkClientException]
        err.getMessage should startWith("Unable to execute HTTP request")
      }

      it("errors if the key doesn't exist") {
        withLocalS3Bucket { bucket =>
          val location = createObjectLocationWith(bucket.name)
          withStoreImpl(initialEntries = Map.empty) { store =>
            val err = store.get(location).left.value
            err shouldBe a[DoesNotExistError]

            err.e shouldBe a[AmazonS3Exception]
            err.e.getMessage should startWith("The specified key does not exist")
          }
        }
      }

      it("errors if the bucket doesn't exist") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          val err = store.get(createObjectLocationWith(createBucketName)).left.value
          err shouldBe a[DoesNotExistError]

          err.e shouldBe a[AmazonS3Exception]
          err.e.getMessage should startWith("The specified bucket does not exist")
        }
      }

      it("errors if asked to get from an invalid bucket") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          val err = store.get(createObjectLocationWith(namespace = "ABCD")).left.value
          err shouldBe a[StoreReadError]

          err.e shouldBe a[AmazonS3Exception]
          err.e.getMessage should startWith("The specified bucket is not valid")
        }
      }
    }

    describe("put") {
      it("errors if S3 fails to respond") {
        val store = new S3StreamStore()(brokenS3Client)

        val result = store.put(createObjectLocation)(createT).left.value
        result shouldBe a[StoreWriteError]

        val err = result.e
        err shouldBe a[SdkClientException]
        err.getMessage should startWith("Unable to execute HTTP request")
      }

      it("errors if the bucket doesn't exist") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          val result = store.put(createObjectLocation)(createT).left.value

          result shouldBe a[StoreWriteError]

          val err = result.e
          err shouldBe a[AmazonS3Exception]
          err.getMessage should startWith("The specified bucket is not valid")
        }
      }

      it("errors if the object key is too long") {
        withNamespace { implicit namespace =>

          // Maximum length of an s3 key is 1024 bytes as of 25/06/2019
          // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

          val tooLongPath = randomStringOfByteLength(1025)()
          val id = createId.copy(path = tooLongPath)

          val entry = ReplayableStream(randomBytes(), metadata = Map.empty)

          withStoreImpl(initialEntries = Map.empty) { store =>
            val value = store.put(id)(entry).left.value

            value shouldBe a[InvalidIdentifierFailure]
            value.e.getMessage should startWith("S3 object key byte length is too big")
          }
        }
      }
    }
  }
}
