package uk.ac.wellcome.storage.vhs

import java.io.{ByteArrayInputStream, InputStream}

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.LocalVersionedHybridStore
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class StreamStoreVersionedHybridStoreTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with LocalVersionedHybridStore {

  import uk.ac.wellcome.storage.dynamo._

  val emptyMetadata = EmptyMetadata()

  private def stringify(is: InputStream) =
    scala.io.Source.fromInputStream(is).mkString

  def withStreamVHS[Metadata, R](
    bucket: Bucket,
    table: Table,
    globalS3Prefix: String = defaultGlobalS3Prefix)(
    testWith: TestWith[
      VersionedHybridStore[InputStream, Metadata, ObjectStore[InputStream]],
      R])(
    implicit objectStore: ObjectStore[InputStream]
  ): R = {
    val vhsConfig = createVHSConfigWith(
      table = table,
      bucket = bucket,
      globalS3Prefix = globalS3Prefix
    )

    val store = new VersionedHybridStore[
      InputStream,
      Metadata,
      ObjectStore[InputStream]](
      vhsConfig = vhsConfig,
      objectStore = objectStore,
      dynamoDbClient = dynamoDbClient
    )

    testWith(store)
  }

  def withS3StreamStoreFixtures[R](
    testWith: TestWith[(Table,
                        VersionedHybridStore[InputStream,
                                             EmptyMetadata,
                                             ObjectStore[InputStream]]),
                       R]): R =
    withLocalS3Bucket[R] { bucket =>
      withLocalDynamoDbTable[R] { table =>
        withStreamVHS[EmptyMetadata, R](bucket, table) { vhs =>
          testWith((table, vhs))
        }
      }
    }

  describe("with S3StreamStore") {
    it("stores an InputStream") {
      withS3StreamStoreFixtures {
        case (table, hybridStore) =>
          val id = Random.nextString(5)
          val content = "A thousand thinking thanes thanking a therapod"
          val inputStream = new ByteArrayInputStream(content.getBytes)

          val future = hybridStore.updateRecord(id)(ifNotExisting =
            (inputStream, emptyMetadata))(ifExisting = (t, m) => (t, m))

          whenReady(future) { _ =>
            getContentFor(table, id) shouldBe content
          }
      }
    }

    it("retrieves an InputStream") {
      withS3StreamStoreFixtures {
        case (_, hybridStore) =>
          val id = Random.nextString(5)
          val content = "Five fishing flinging flint"
          val inputStream = new ByteArrayInputStream(content.getBytes)

          val putFuture =
            hybridStore.updateRecord(id)(ifNotExisting =
              (inputStream, emptyMetadata))(ifExisting = (t, m) => (t, m))

          val getFuture = putFuture.flatMap { _ =>
            hybridStore.getRecord(id)
          }

          whenReady(getFuture) { result =>
            result.map(stringify) shouldBe Some(content)
          }
      }
    }
  }
}
