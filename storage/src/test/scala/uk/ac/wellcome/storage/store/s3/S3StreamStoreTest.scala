package uk.ac.wellcome.storage.store.s3

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures

class S3StreamStoreTest
  extends StreamStoreTestCases[S3ObjectLocation, Bucket, S3StreamStore, Unit]
    with S3StreamStoreFixtures
    with BucketNamespaceFixtures {
  describe("handles errors from S3") {
    describe("get") {
      it("errors if S3 has a problem") {
        val store = new S3StreamStore()(brokenS3Client)

        val result = store.get(createS3ObjectLocation).left.value
        result shouldBe a[StoreReadError]

        val err = result.e
        err shouldBe a[SdkClientException]
        err.getMessage should startWith("Unable to execute HTTP request")
      }

      it("errors if the key doesn't exist") {
        withLocalS3Bucket { bucket =>
          val location = createS3ObjectLocationWith(bucket)
          withStoreImpl(initialEntries = Map.empty) { store =>
            store.get(location).left.value shouldBe a[DoesNotExistError]
          }
        }
      }

      it("errors if the bucket doesn't exist") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          store.get(createS3ObjectLocation).left.value shouldBe a[DoesNotExistError]
        }
      }
    }

    describe("put") {
      it("errors if S3 fails to respond") {
        val store = new S3StreamStore()(brokenS3Client)

        val result = store.put(createS3ObjectLocation)(createT).left.value
        result shouldBe a[StoreWriteError]

        val err = result.e
        err shouldBe a[SdkClientException]
        err.getMessage should startWith("Unable to execute HTTP request")
      }

      it("errors if the bucket doesn't exist") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          val result = store.put(createS3ObjectLocation)(createT).left.value

          result shouldBe a[StoreWriteError]

          val err = result.e
          err shouldBe a[AmazonS3Exception]
          err.getMessage should startWith("The specified bucket is not valid")
        }
      }

      it("errors if the object key is too long") {
        withLocalS3Bucket { bucket =>

          // Maximum length of an s3 key is 1024 bytes as of 25/06/2019
          // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

          val tooLongKey = randomStringOfByteLength(1025)()
          val location = createS3ObjectLocationWith(
            bucket = bucket,
            key = tooLongKey
          )

          val entry = ReplayableStream(randomBytes(), metadata = Map.empty)

          withStoreImpl(initialEntries = Map.empty) { store =>
            val value = store.put(location)(entry).left.value

            value shouldBe a[InvalidIdentifierFailure]
            value.e.getMessage should startWith("S3 object key byte length is too big")
          }
        }
      }
    }
  }
}
