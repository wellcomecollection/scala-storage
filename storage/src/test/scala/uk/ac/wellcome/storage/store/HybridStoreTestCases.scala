package uk.ac.wellcome.storage.store

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, InputStreamWithLengthAndMetadata}
import uk.ac.wellcome.storage._

trait HybridStoreTestCases[IndexedStoreId, TypedStoreId, T, Metadata, Namespace,
TypedStoreImpl <: TypedStore[TypedStoreId, T],
IndexedStoreImpl <: Store[IndexedStoreId, HybridIndexedStoreEntry[IndexedStoreId, TypedStoreId, Metadata]],
HybridStoreContext] extends FunSpec with StoreTestCases[IndexedStoreId, HybridStoreEntry[T, Metadata], Namespace, HybridStoreContext]
  with Matchers
  with RandomThings
  with EitherValues {

  type HybridStoreImpl = HybridStore[IndexedStoreId, TypedStoreId, T, Metadata]

  def withHybridStoreImpl[R](typedStore: TypedStoreImpl, indexedStore: IndexedStoreImpl)(testWith: TestWith[HybridStoreImpl, R])(implicit context: HybridStoreContext): R

  def withTypedStoreImpl[R](testWith: TestWith[TypedStoreImpl, R])(implicit context: HybridStoreContext): R

  def withIndexedStoreImpl[R](testWith: TestWith[IndexedStoreImpl, R])(implicit context: HybridStoreContext): R

  def createTypedStoreId(implicit namespace: Namespace): TypedStoreId

  def createIndexedStoreId(implicit namespace: Namespace): IndexedStoreId = createId

  def createMetadata: Metadata

  def withBrokenPutTypedStoreImpl[R](testWith: TestWith[TypedStoreImpl, R])(implicit context: HybridStoreContext): R

  def withBrokenGetTypedStoreImpl[R](testWith: TestWith[TypedStoreImpl, R])(implicit context: HybridStoreContext): R

  def withBrokenPutIndexedStoreImpl[R](testWith: TestWith[IndexedStoreImpl, R])(implicit context: HybridStoreContext): R

  def withBrokenGetIndexedStoreImpl[R](testWith: TestWith[IndexedStoreImpl, R])(implicit context: HybridStoreContext): R

  override def withStoreImpl[R](initialEntries: Map[IndexedStoreId, HybridStoreEntry[T, Metadata]], storeContext: HybridStoreContext)(testWith: TestWith[StoreImpl, R]): R = {
    implicit val context: HybridStoreContext = storeContext

    withTypedStoreImpl { typedStore =>
      withIndexedStoreImpl { indexedStore =>
        withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
          initialEntries.map { case (id, entry) =>
            hybridStore.put(id)(entry) shouldBe a[Right[_, _]]
          }

          testWith(hybridStore)
        }
      }
    }
  }

  describe("it behaves as a HybridStore") {
    describe("storing a new record") {
      it("stores the object in the object store") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  val putResult = hybridStore.put(id)(hybridStoreEntry)
                  val putValue = putResult.right.value

                  val indexedResult = indexedStore.get(putValue.id)
                  val indexedValue = indexedResult.right.value

                  val typedStoreId = indexedValue.identifiedT.typedStoreId

                  val typedResult = typedStore.get(typedStoreId)
                  val typedValue = typedResult.right.value
                  typedValue.identifiedT shouldBe TypedStoreEntry(hybridStoreEntry.t, Map.empty)
                }
              }
            }
          }
        }
      }

      it("stores the metadata in the indexed store") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val id = createId
                  val hybridStoreEntry = createT

                  val putResult = hybridStore.put(id)(hybridStoreEntry)
                  val putValue = putResult.right.value

                  val indexedResult = indexedStore.get(putValue.id)
                  val indexedValue = indexedResult.right.value

                  indexedValue.id shouldBe id
                  indexedValue.identifiedT.metadata shouldBe hybridStoreEntry.metadata
                }
              }
            }
          }
        }
      }
    }

    describe("handles errors in the underlying stores") {
      it("if the typed store has a write error") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withBrokenPutTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) {
                  _.put(createId)(createT).left.value shouldBe a[WriteError]
                }
              }
            }
          }
        }
      }

      it("if the indexed store has a write error") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withBrokenPutIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) {
                  _.put(createId)(createT).left.value shouldBe a[WriteError]
                }
              }
            }
          }
        }
      }

      it("if the indexed store refers to a missing typed store entry") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>

                val indexedStoreId = createIndexedStoreId
                val typedStoreId = createTypedStoreId
                val metadata = createMetadata

                val hybridIndexedStoreEntry = HybridIndexedStoreEntry(
                  indexedStoreId = indexedStoreId,
                  typedStoreId = typedStoreId,
                  metadata = metadata
                )

                indexedStore.put(indexedStoreId)(hybridIndexedStoreEntry) shouldBe a[Right[_, _]]

                withHybridStoreImpl(typedStore, indexedStore) {
                  _.get(indexedStoreId).left.value shouldBe a[DanglingHybridStorePointerError]
                }
              }
            }
          }
        }
      }

      it("if the typed store has a read error") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withBrokenGetTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>

                val indexedStoreId = createIndexedStoreId
                val typedStoreId = createTypedStoreId
                val metadata = createMetadata

                val hybridIndexedStoreEntry = HybridIndexedStoreEntry(
                  indexedStoreId = indexedStoreId,
                  typedStoreId = typedStoreId,
                  metadata = metadata
                )

                indexedStore.put(indexedStoreId)(hybridIndexedStoreEntry) shouldBe a[Right[_, _]]

                withHybridStoreImpl(typedStore, indexedStore) { hybridStore =>
                  val err = hybridStore.get(indexedStoreId).left.value
                  err shouldBe a[ReadError]
                  err.isInstanceOf[DoesNotExistError] shouldBe false
                }
              }
            }
          }
        }
      }

      it("if the indexed store has a read error") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withBrokenGetIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) {
                  _.get(createId).left.value shouldBe a[ReadError]
                }
              }
            }
          }
        }
      }

      it("if the data in the typed store is the wrong format") {
        withStoreContext { implicit context =>
          withNamespace { implicit namespace =>
            withTypedStoreImpl { typedStore =>
              withIndexedStoreImpl { indexedStore =>
                withHybridStoreImpl(typedStore, indexedStore) { hybridStoreImpl =>
                  val id = createId

                  hybridStoreImpl.put(id)(createT) shouldBe a[Right[_, _]]

                  val typeStoreId = indexedStore.get(id).right.value
                    .identifiedT.typedStoreId

                  val byteLength = 256
                  val inputStream = randomInputStream(byteLength)
                  val inputStreamWithLength = new InputStreamWithLength(inputStream, byteLength)

                  val inputStreamWithLengthAndMetadata =
                    InputStreamWithLengthAndMetadata(inputStreamWithLength, Map.empty)

                  typedStore.streamStore.put(typeStoreId)(inputStreamWithLengthAndMetadata) shouldBe a[Right[_,_]]

                  val value = hybridStoreImpl.get(id).left.value

                  value shouldBe a[JsonDecodingError]
                }
              }
            }
          }
        }
      }
    }
  }
}

trait HybridStoreWithOverwritesTestCases[
IndexedStoreId, TypedStoreId, T, Metadata, Namespace,
TypedStoreImpl <: TypedStore[TypedStoreId, T],
IndexedStoreImpl <: Store[IndexedStoreId, HybridIndexedStoreEntry[IndexedStoreId, TypedStoreId, Metadata]],
HybridStoreContext]
  extends HybridStoreTestCases[IndexedStoreId, TypedStoreId, T, Metadata, Namespace, TypedStoreImpl, IndexedStoreImpl, HybridStoreContext]
    with StoreWithOverwritesTestCases[IndexedStoreId, HybridStoreEntry[T, Metadata], Namespace, HybridStoreContext]

trait HybridStoreWithoutOverwritesTestCases[
IndexedStoreId, TypedStoreId, T, Metadata, Namespace,
TypedStoreImpl <: TypedStore[TypedStoreId, T],
IndexedStoreImpl <: Store[IndexedStoreId, HybridIndexedStoreEntry[IndexedStoreId, TypedStoreId, Metadata]],
HybridStoreContext]
  extends HybridStoreTestCases[IndexedStoreId, TypedStoreId, T, Metadata, Namespace, TypedStoreImpl, IndexedStoreImpl, HybridStoreContext]
    with StoreWithoutOverwritesTestCases[IndexedStoreId, HybridStoreEntry[T, Metadata], Namespace, HybridStoreContext]