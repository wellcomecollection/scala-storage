package uk.ac.wellcome.storage.store

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.{ReadError, WriteError}

trait HybridStoreTestCases[IndexedStoreId, TypeStoreId, T, Metadata, Namespace,
TypedStoreImpl <: TypedStore[TypeStoreId, T],
IndexedStoreImpl <: Store[IndexedStoreId, HybridIndexedStoreEntry[IndexedStoreId, TypeStoreId, Metadata]],
HybridStoreImpl <: HybridStore[IndexedStoreId, TypeStoreId, T, Metadata]
] extends FunSpec with StoreTestCases[IndexedStoreId, HybridStoreEntry[T, Metadata], Namespace, (TypedStoreImpl, IndexedStoreImpl)]
  with Matchers
  with RandomThings
  with EitherValues {

  type StoreContext = (TypedStoreImpl, IndexedStoreImpl)

  def createHybridStoreWith(context: StoreContext): HybridStoreImpl

  def createTypedStoreId: TypeStoreId

  def createIndexedStoreId(implicit namespace: Namespace): IndexedStoreId = createId

  def createMetadata: Metadata

  override def withStoreImpl[R](initialEntries: Map[IndexedStoreId, HybridStoreEntry[T, Metadata]])(testWith: TestWith[StoreImpl, R]): R =
    withStoreContext { storeContext =>
      val hybridStore = createHybridStoreWith(storeContext)

      initialEntries.foreach {
        case (id, entry: HybridStoreEntry[T, Metadata]) => {
          hybridStore.put(id)(entry) shouldBe a[Right[_,_]]
        }
      }

      testWith(createHybridStoreWith(storeContext))
    }

  override def withStoreImpl[R](storeContext: StoreContext, initialEntries: Map[IndexedStoreId, HybridStoreEntry[T, Metadata]])(testWith: TestWith[StoreImpl, R]): R = testWith(
    createHybridStoreWith(storeContext))

  describe("HybridStore") {
    describe("storing a new record") {
      withStoreContext { case impls@(typedStore, indexedStore) =>
        withStoreImpl(impls, Map.empty) { hybridStore =>
          withNamespace { implicit namespace =>

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

    describe("errors when storing the record") {
      it("fails if the object store has an error") {
        withStoreContext { case (_, indexedStore) =>
          withNamespace { implicit namespace =>

            val impls = (createBrokenPutTypedStore, indexedStore)

            withStoreImpl(impls, Map.empty) {
              _.put(
                createId)(createT).left.value shouldBe a[WriteError]
            }
          }
        }
      }

      it("fails if the indexed store has an error") {
        withStoreContext { case (objectStore, _) =>
          withNamespace { implicit namespace =>

            val impls = (objectStore, createBrokenPutIndexedStore)
            withStoreImpl(impls, Map.empty) {
              _.put(createId)(createT).left.value shouldBe a[WriteError]
            }
          }
        }
      }
    }

    describe("errors when getting the record") {
      it("fails if the indexed store refers to a missing typed store entry") {
        withStoreContext { case impls@(_, indexedStore) =>
          withNamespace { implicit namespace =>

            val hybridStoreId, indexedStoreId = createIndexedStoreId
            val typedStoreId = createTypedStoreId
            val metadata = createMetadata

            val hybridIndexedStoreEntry =
              HybridIndexedStoreEntry(indexedStoreId, typedStoreId, metadata)

            indexedStore.put(indexedStoreId)(hybridIndexedStoreEntry) shouldBe a[Right[_, _]]

            withStoreImpl(impls, Map.empty) {
              _.get(hybridStoreId).left.value shouldBe a[ReadError]
            }
          }
        }
      }

      it("fails if the typed store has an error") {
        withStoreContext { case (_, indexedStore) =>
          withNamespace { implicit namespace =>

            val hybridStoreId, indexedStoreId = createIndexedStoreId
            val typedStoreId = createTypedStoreId
            val metadata = createMetadata

            val hybridIndexedStoreEntry =
              HybridIndexedStoreEntry(indexedStoreId, typedStoreId, metadata)

            indexedStore.put(indexedStoreId)(hybridIndexedStoreEntry) shouldBe a[Right[_, _]]

            val impls = (createBrokenGetTypedStore, indexedStore)

            withStoreImpl(impls, Map.empty) {
              _.get(hybridStoreId).left.value shouldBe a[ReadError]
            }
          }
        }
      }

      it("fails if the indexed store has an error") {
        withStoreContext { case (objectStore, _) =>
          withNamespace { implicit namespace =>
            val impls = (objectStore, createBrokenGetIndexedStore)
            withStoreImpl(impls, Map.empty) {
              _.get(createId).left.value shouldBe a[ReadError]
            }
          }
        }
      }
    }
  }

  def createBrokenPutTypedStore: TypedStoreImpl

  def createBrokenPutIndexedStore: IndexedStoreImpl

  def createBrokenGetTypedStore: TypedStoreImpl

  def createBrokenGetIndexedStore: IndexedStoreImpl
}
