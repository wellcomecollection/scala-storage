package uk.ac.wellcome.storage.store

import org.scalatest.{EitherValues, FunSpec}
import uk.ac.wellcome.storage.{DoesNotExistError, Identified}

trait StoreTestCases[Id, T, Namespace, StoreContext] extends FunSpec with EitherValues with StoreFixtures[Id, T, Namespace, StoreContext] {
  describe("it behaves as a store") {
    describe("get") {
      it("fails to get a non-existent location") {
        withNamespace { implicit namespace =>
          withEmptyStoreImpl { store =>
            store.get(createId).left.value shouldBe a[DoesNotExistError]
          }
        }
      }

      it("can get a stored T") {
        withNamespace { implicit namespace =>
          val id = createId
          val t = createT

          withStoreImpl(initialEntries = Map(id -> t)) { store =>
            val storedEntry = store.get(id).right.value

            storedEntry shouldBe a[Identified[_, _]]
            storedEntry.id shouldBe id
            assertEqualT(t, storedEntry.identifiedT)
          }
        }
      }
    }

    describe("put") {
      it("can put a T") {
        withNamespace { implicit namespace =>
          val id = createId
          val t = createT

          withStoreImpl(initialEntries = Map.empty) { store =>
            store.put(id)(t) shouldBe a[Right[_, _]]
          }
        }
      }

      it("can overwrite an existing entry") {
        withNamespace { implicit namespace =>
          val id = createId
          val t1 = createT
          val t2 = createT

          withEmptyStoreImpl { store =>
            store.put(id)(t1) shouldBe a[Right[_, _]]
            store.put(id)(t2) shouldBe a[Right[_, _]]
          }
        }
      }
    }

    it("persists entries over instances of the store") {
      withStoreContext { storeContext =>
        withNamespace { implicit namespace =>

          val id = createId
          val t = createT

          withStoreImpl(storeContext, initialEntries = Map.empty) { store1 =>
            store1.put(id)(t) shouldBe a[Right[_, _]]
          }

          withStoreImpl(storeContext, initialEntries = Map.empty) { store2 =>
            val retrievedEntry: Identified[Id, T] = store2.get(id).right.value
            retrievedEntry.id shouldBe id
            assertEqualT(t, retrievedEntry.identifiedT)
          }
        }
      }
    }

    it("is internally consistent") {
      withNamespace { implicit namespace =>
        val id = createId

        val t1 = createT
        val t2 = createT

        withEmptyStoreImpl { store =>
          store.put(id)(t1) shouldBe a[Right[_, _]]

          val storedEntry1 = store.get(id).right.value

          storedEntry1 shouldBe a[Identified[_, _]]
          storedEntry1.id shouldBe id
          assertEqualT(t1, storedEntry1.identifiedT)

          store.put(id)(t2) shouldBe a[Right[_, _]]

          val storedEntry2 = store.get(id).right.value

          storedEntry2 shouldBe a[Identified[_, _]]
          storedEntry2.id shouldBe id
          assertEqualT(t2, storedEntry2.identifiedT)
        }
      }
    }
  }
}
