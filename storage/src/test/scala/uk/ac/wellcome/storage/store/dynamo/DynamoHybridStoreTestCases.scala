package uk.ac.wellcome.storage.store.dynamo

import org.scanamo.auto._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store._
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}

trait DynamoHybridStoreTestCases[DynamoStoreImpl <: Store[Version[String, Int], HybridIndexedStoreEntry[ObjectLocation, Map[String, String]]]]
  extends HybridStoreWithoutOverwritesTestCases[
    Version[String, Int],
    ObjectLocation,
    Record,
    Map[String, String],
    Unit,
    S3TypedStore[Record],
    DynamoStoreImpl,
    (Bucket, Table)
    ] with RecordGenerators with S3Fixtures with DynamoFixtures with MetadataGenerators {
  type S3TypedStoreImpl = S3TypedStore[Record]
  type DynamoIndexedStoreImpl = DynamoStoreImpl
  type IndexedStoreEntry = HybridIndexedStoreEntry[ObjectLocation, Map[String, String]]

  def createPrefix(implicit context: (Bucket, Table)): ObjectLocationPrefix = {
    val (bucket, _) = context
    ObjectLocationPrefix(
      namespace = bucket.name,
      path = randomAlphanumeric
    )
  }

  override def withTypedStoreImpl[R](testWith: TestWith[S3TypedStoreImpl, R])(implicit context: (Bucket, Table)): R =
    testWith(S3TypedStore[Record])

  override def createTypedStoreId(implicit bucket: Unit): ObjectLocation =
    createObjectLocation

  override def createMetadata: Map[String, String] = createValidMetadata

  override def withBrokenPutTypedStoreImpl[R](testWith: TestWith[S3TypedStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    val s3StreamStore: S3StreamStore = new S3StreamStore()

    testWith(
      new S3TypedStore[Record]()(codec, s3StreamStore) {
        override def put(id: ObjectLocation)(entry: TypedStoreEntry[Record]): WriteEither =
          Left(StoreWriteError(new Error("BOOM!")))
      }
    )
  }

  override def withBrokenGetTypedStoreImpl[R](testWith: TestWith[S3TypedStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    val s3StreamStore: S3StreamStore = new S3StreamStore()

    testWith(
      new S3TypedStore[Record]()(codec, s3StreamStore) {
        override def get(id: ObjectLocation): ReadEither =
          Left(StoreReadError(new Error("BOOM!")))
      }
    )
  }

  override def withStoreContext[R](testWith: TestWith[(Bucket, Table), R]): R =
    withLocalS3Bucket { bucket =>
      withLocalDynamoDbTable { table =>
        testWith((bucket, table))
      }
    }

  override def createT: HybridStoreEntry[Record, Map[String, String]] =
    HybridStoreEntry(createRecord, createValidMetadata)

  override def withNamespace[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def createId(implicit namespace: Unit): Version[String, Int] =
    Version(id = randomAlphanumeric, version = 1)

  describe("DynamoHybridStore") {
    it("appends a .json suffix to object keys") {
      withStoreContext { implicit context =>
        withNamespace { implicit namespace =>
          withTypedStoreImpl { typedStore =>
            withIndexedStoreImpl { indexedStore =>
              withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                val id = createId
                val hybridStoreEntry = createT

                val putResult = hybridStore.put(id)(hybridStoreEntry)
                val putValue = putResult.right.value

                val dynamoResult = indexedStore.get(putValue.id)
                val dynamoValue = dynamoResult.right.value

                val s3Location = dynamoValue.identifiedT.typedStoreId

                s3Location.path should endWith(".json")
              }
            }
          }
        }
      }
    }

    describe("it handles errors from AWS") {
      it("if the prefix refers to a non-existent bucket") {
        withStoreContext { case (_, table) =>
          val nonExistentBucket = Bucket(randomAlphanumeric)

          implicit val context = (nonExistentBucket, table)

          withNamespace { implicit namespace =>

            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  val result = hybridStore.put(id)(hybridStoreEntry)
                  val value = result.left.value

                  value shouldBe a[StoreWriteError]
                  value.e.getMessage should startWith("The specified bucket is not valid")
                }
              }
            }

          }
        }
      }

      it("if the prefix creates an S3 key that's too long") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>

                  // Maximum length of an s3 key is 1024 bytes as of 25/06/2019
                  // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

                  // The hybrid store appends _some_ value to this path.
                  // This test sets the id length to 1024 expecting the
                  // implementation to append >0 bytes thereby causing
                  // a failure.

                  // There is also a dynamo hash/range key restriction but
                  // this is (at time of writing) greater than the s3 key
                  // length restriction and cannot be reached without
                  // invoking this error.

                  val tooLongId = randomStringOfByteLength(1024)()

                  val id = Version(id = tooLongId, version = 1)
                  val hybridStoreEntry = createT

                  val result = hybridStore.put(id)(hybridStoreEntry)
                  val value = result.left.value

                  value shouldBe a[InvalidIdentifierFailure]
                  value.e.getMessage should startWith("S3 object key byte length is too big")
                }
              }
            }
          }
        }
      }

      it("if the underlying DynamoDB table doesn't exist") {
        withStoreContext { case (bucket, _) =>
          val nonExistentTable = Table(randomAlphanumeric, randomAlphanumeric)

          implicit val context = (bucket, nonExistentTable)

          withNamespace { implicit namespace =>

            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  val result = hybridStore.put(id)(hybridStoreEntry)
                  val value = result.left.value

                  value shouldBe a[StoreWriteError]
                  value.e.getMessage should startWith("Cannot do operations on a non-existent table")
                }
              }
            }

          }
        }
      }

      it("if a DynamoDB index entry points to a non-existent S3 key") {
        withStoreContext { case (bucket, table) =>
          withNamespace { implicit namespace =>

            implicit val context = (bucket, table)

            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  hybridStore.put(id)(hybridStoreEntry) shouldBe a[Right[_,_]]

                  val indexedEntry = indexedStore.get(id).right.value
                  val typeStoreId = indexedEntry.identifiedT.typedStoreId

                  s3Client.deleteObject(typeStoreId.namespace, typeStoreId.path)

                  val value = hybridStore.get(id).left.value

                  value shouldBe a[DanglingHybridStorePointerError]
                }
              }
            }
          }
        }
      }

      it("if a DynamoDB index entry points to a non-existent S3 bucket") {
        withStoreContext { case (bucket, table) =>
          withNamespace { implicit namespace =>

            implicit val context = (bucket, table)

            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  hybridStore.put(id)(hybridStoreEntry) shouldBe a[Right[_,_]]

                  val indexedEntry = indexedStore.get(id).right.value
                  val typeStoreId = indexedEntry.identifiedT.typedStoreId

                  s3Client.deleteObject(typeStoreId.namespace, typeStoreId.path)
                  s3Client.deleteBucket(typeStoreId.namespace)

                  val value = hybridStore.get(id).left.value

                  value shouldBe a[StoreReadError]
                  value.e.getMessage should startWith("The specified bucket does not exist")
                }
              }
            }
          }
        }
      }

      it("if the DynamoDB row is in the wrong format") {
        withStoreContext { case (bucket, table) =>
          withNamespace { implicit namespace =>

            implicit val context = (bucket, table)

            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId

                  case class BadRow(id: String, version: Int, contents: String)

                  putTableItem(BadRow(id.id, id.version, randomAlphanumeric), table)

                  val value = hybridStore.get(id).left.value

                  value shouldBe a[StoreReadError]
                  value.e.getMessage should startWith("DynamoReadError")
                }
              }
            }
          }
        }
      }
    }
  }
}
