package uk.ac.wellcome.storage.store.s3

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.TypedStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class S3TypedStoreTest
    extends TypedStoreTestCases[
      ObjectLocation,
      Record,
      Bucket,
      S3StreamStore,
      S3TypedStore[Record],
      Unit]
    with S3TypedStoreFixtures[Record]
    with RecordGenerators
    with BucketNamespaceFixtures {
  override def withBrokenStreamStore[R](
    testWith: TestWith[S3StreamStore, R]): R = {
    val brokenS3StreamStore = new S3StreamStore {
      override def get(location: ObjectLocation): ReadEither = Left(
        StoreReadError(new Throwable("get: BOOM!"))
      )

      override def put(location: ObjectLocation)(
        inputStream: InputStreamWithLength): WriteEither = Left(
        StoreWriteError(
          new Throwable("put: BOOM!")
        )
      )
    }

    testWith(brokenS3StreamStore)
  }

  override def withSingleValueStreamStore[R](rawStream: InputStream)(
    testWith: TestWith[S3StreamStore, R]): R = {
    val s3StreamStore: S3StreamStore = new S3StreamStore() {
      override def get(location: ObjectLocation): ReadEither =
        Right(
          Identified(
            location,
            new InputStreamWithLength(rawStream, length = 0)
          )
        )
    }

    testWith(s3StreamStore)
  }

  override def createT: Record =
    createRecord

  describe("S3TypedStore") {
    it("errors if the object key is too long") {
      withNamespace { implicit namespace =>
        // Maximum length of an s3 key is 1024 bytes as of 25/06/2019
        // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

        val tooLongPath = randomStringOfByteLength(1025)()
        val id = createId.copy(path = tooLongPath)

        val entry = createT

        withStoreImpl(initialEntries = Map.empty) { store =>
          val value = store.put(id)(entry).left.value

          value shouldBe a[InvalidIdentifierFailure]
          value.e.getMessage should startWith(
            "S3 object key byte length is too big")
        }
      }
    }
  }
}
