package uk.ac.wellcome.storage.store.s3

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation, StoreReadError, StoreWriteError}

class S3StreamStoreTest extends StreamStoreTestCases[ObjectLocation, Bucket, S3StreamingStore, Unit] with S3StreamStoreFixtures with BucketNamespaceFixtures {
  describe("handles errors from S3") {
    describe("get") {
      it("errors if S3 has a problem") {
        val store = new S3StreamingStore()(brokenS3Client)

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
            store.get(location).left.value shouldBe a[DoesNotExistError]
          }
        }
      }

      it("errors if the bucket doesn't exist") {
        withStoreImpl(initialEntries = Map.empty) { store =>
          store.get(createObjectLocation).left.value shouldBe a[DoesNotExistError]
        }
      }
    }

    describe("put") {
      it("errors if S3 fails to respond") {
        val store = new S3StreamingStore()(brokenS3Client)

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
    }
  }
}
