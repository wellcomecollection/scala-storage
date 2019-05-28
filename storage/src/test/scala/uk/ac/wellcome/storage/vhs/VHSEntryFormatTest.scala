package uk.ac.wellcome.storage.vhs

import com.gu.scanamo.Scanamo
import com.gu.scanamo.syntax._
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.{ObjectStore, StorageBackend, VersionedDao}
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDbVersioned, S3}
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.streaming.CodecInstances._

class VHSEntryFormatTest extends FunSpec with Matchers with LocalDynamoDbVersioned with S3 {
  case class Container(shape: String, volume: Int)

  it("allows storing VHS entries with EmptyMetadata in DynamoDB") {
    val store = new ObjectStore[Container] {
      override implicit val codec: Codec[Container] = typeCodec[Container]
      override implicit val storageBackend: StorageBackend = s3StorageBackend
    }

    withLocalDynamoDbTable { table =>
      val dao = DynamoVersionedDao[String, Entry[String, EmptyMetadata]](
        dynamoClient = dynamoDbClient,
        dynamoConfig = createDynamoConfigWith(table)
      )

      withLocalS3Bucket { bucket =>
        val vhs = new VersionedHybridStore[String, Container, EmptyMetadata] {
          override val namespace: String = bucket.name
          override protected val versionedDao: VersionedDao[String, VHSEntry] = dao
          override protected val objectStore: ObjectStore[Container] = store
        }

        vhs.update(id = "1")(
          ifNotExisting = (Container(shape = "circle", volume = 5), EmptyMetadata())
        )(
          ifExisting = (_, _) => (Container(shape = "circle", volume = 5), EmptyMetadata())
        ) shouldBe a[Right[_, _]]

        dao.underlying.get(id = "1").right.value shouldBe a[Entry[_, _]]

        Scanamo.get[PlainEntry[String]](dynamoDbClient)(table.name)('id -> "1").get shouldBe a[Right[_, _]]
      }
    }
  }
}
