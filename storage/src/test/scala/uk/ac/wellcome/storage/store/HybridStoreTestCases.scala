package uk.ac.wellcome.storage.store

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.{DanglingHybridStorePointerError, DoesNotExistError, ReadError, WriteError}

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

  override def withStoreImpl[R](storeContext: HybridStoreContext, initialEntries: Map[IndexedStoreId, HybridStoreEntry[T, Metadata]])(testWith: TestWith[StoreImpl, R]): R = {
    // TODO: The underlying withStoreImpl method should take implicit context
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

                val typedStoreId = indexedValue.identifiedT.typeStoreId

                val typedResult = typedStore.get(typedStoreId)
                val typedValue = typedResult.right.value

                it("stores the object in the object store") {
                  typedValue.identifiedT shouldBe TypedStoreEntry(hybridStoreEntry.t, Map.empty)
                }

                it("stores the metadata in the indexed store") {
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

                // TODO: Change the second parameter to 'typedStoreId'
                val hybridIndexedStoreEntry = HybridIndexedStoreEntry(
                  indexedStoreId = indexedStoreId,
                  typeStoreId = typedStoreId,
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
                  typeStoreId = typedStoreId,
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
    }
  }
}
