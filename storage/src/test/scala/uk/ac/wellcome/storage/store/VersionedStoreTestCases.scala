package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.fixtures.VersionedStoreFixtures

trait VersionedStoreTestCases[Id, T] extends FunSpec with Matchers with EitherValues with Logging with VersionedStoreFixtures[Id, T] {
  def createIdent: Id
  def createT: T

  type Entries = Map[Version[Id, Int], T]

  def withVersionedStore[R](initialEntries: Entries = Map.empty)(testWith: TestWith[VersionedStoreImpl, R]): R

  def withFailingGetVersionedStore[R](initialEntries: Entries = Map.empty)(testWith: TestWith[VersionedStoreImpl, R]): R
  def withFailingPutVersionedStore[R](initialEntries: Entries = Map.empty)(testWith: TestWith[VersionedStoreImpl, R]): R

  describe("it behaves as a VersionedStore") {
    describe("init") {
      it("stores a new record at the starting version") {
        withVersionedStore() { dao =>
          val id = createIdent
          val t = createT

          val result = dao.init(id)(t)
          val value = result.right.value

          value.identifiedT shouldBe t
          value.id shouldBe Version(id, 0)
        }
      }

      it("does not store a new record if one exists") {
        val id = createIdent

        val t1 = createT
        val t2 = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t1)
        ) { store =>
          val result = store.init(id)(t2)
          val err = result.left.value

          err shouldBe a[VersionAlreadyExistsError]
        }
      }

      it("fails if the underlying dao fails to put") {
        withFailingPutVersionedStore() { store =>
          val id = createIdent
          val t = createT

          val result = store.init(id)(t)
          result.left.value shouldBe a[StoreWriteError]
        }
      }
    }

    describe("put") {
      it("stores a new record") {
        withVersionedStore() { store =>
          val id = createIdent

          val t = createT

          val result = store.putLatest(id)(t)

          debug(s"Got: $result")

          val value = result.right.value

          value.identifiedT shouldEqual t
          value.id shouldEqual Version(id, 0)
        }
      }

      it("increments version monotonically if no version is specified") {
        val id = createIdent

        val t1 = createT
        val t2 = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t1)
        ) { store =>
          val result = store.putLatest(id)(t2)
          val err = result.right.value

          err.identifiedT shouldEqual t2
          err.id shouldEqual Version(id, 1)
        }
      }

      it("puts to a version if specified and that version represents an increase") {
        val id = createIdent

        val t1 = createT
        val t2 = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t1)
        ) { store =>
          val result = store.put(Version(id, 1))(t2)
          val err = result.right.value

          err.identifiedT shouldEqual t2
          err.id shouldEqual Version(id, 1)
        }
      }

      describe("increments version monotonically where there are gaps in the version sequence") {
        it("version is specified") {
          val id = createIdent

          val t0 = createT
          val t2 = createT

          withVersionedStore(
            initialEntries = Map(Version(id, 0) -> t0)
          ) { store =>
            val result = store.put(Version(id, 2))(t2)
            val value = result.right.value

            value.identifiedT shouldEqual t2
            value.id shouldEqual Version(id, 2)
          }
        }

        it("version is not specified") {
          val id = createIdent

          val t2 = createT
          val t3 = createT

          withVersionedStore(
            initialEntries = Map(Version(id, 2) -> t2)
          ) { store =>
            val result = store.putLatest(id)(t3)
            val value = result.right.value

            value.identifiedT shouldEqual t3
            value.id shouldEqual Version(id, 3)
          }
        }
      }

      it("refuses to add a version lower than the latest version for an id") {
        val id = createIdent

        val t2 = createT
        val t3 = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 3) -> t3)
        ) { store =>
          val result = store.put(Version(id, 1))(t2)
          val err = result.left.value

          err shouldBe a[HigherVersionExistsError]
        }
      }

      it("refuses to put to an existing id & version") {
        val id = createIdent

        val t = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          store.put(Version(id, 0))(t).left.value shouldBe a[VersionAlreadyExistsError]
        }
      }

      it("fails if the underlying dao fails to put") {
        withFailingPutVersionedStore() { store =>
          val id = createIdent
          val t = createT

          val result = store.putLatest(id)(t)
          result.left.value shouldBe a[StoreWriteError]
        }
      }
    }

    describe("get") {
      it("gets a stored record") {
        val id = createIdent
        val t = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val result = store.getLatest(id)
          val value = result.right.value

          value shouldBe Identified(Version(id, 0), t)
        }
      }

      it("gets a stored record at a specified version") {
        val id = createIdent

        val t1 = createT
        val t2 = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 1) -> t1, Version(id, 2) -> t2)
        ) { store =>

          val result = store.get(Version(id, 1))

          debug(s"Got $result")

          result.right.value shouldBe Identified(Version(id, 1), t1)
        }
      }

      it("is possible to get all versions") {
        val id = createIdent

        val t1 = createT
        val t2 = createT
        val t3 = createT
        val t4 = createT
        val t5 = createT

        withVersionedStore(
          initialEntries = Map(
            Version(id, 1) -> t1,
            Version(id, 2) -> t2,
            Version(id, 3) -> t3,
            Version(id, 4) -> t4,
            Version(id, 5) -> t5
          )
        ) { store =>
          store.get(Version(id, 1)).right.value shouldBe Identified(Version(id, 1), t1)
          store.get(Version(id, 2)).right.value shouldBe Identified(Version(id, 2), t2)
          store.get(Version(id, 3)).right.value shouldBe Identified(Version(id, 3), t3)
          store.get(Version(id, 4)).right.value shouldBe Identified(Version(id, 4), t4)
          store.get(Version(id, 5)).right.value shouldBe Identified(Version(id, 5), t5)
        }
      }

      it("fails when getting a non-existent version on an id") {
        val id = createIdent
        val t = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          store.get(Version(id, 0)).right.value shouldBe Identified(Version(id, 0), t)
          store.get(Version(id, 1)).left.value shouldBe a[NoVersionExistsError]
        }
      }

      it("fails when getting a non-existent id and version pair") {
        withVersionedStore() { store =>
          val id = createIdent

          store.get(Version(id, 1)).left.value shouldBe a[NoVersionExistsError]
        }
      }

      it("fails when getting a non-existent id") {
        withVersionedStore() { store =>
          val id = createIdent

          store.getLatest(id).left.value shouldBe a[NoVersionExistsError]
        }
      }

      it("fails if the underlying store fails to get") {
        withFailingGetVersionedStore() { store =>
          val id = createIdent

          store.get(Version(id, 1)).left.value shouldBe a[StoreReadError]
        }
      }
    }

    describe("upsert") {
      it("updates an existing id") {
        val id = createIdent

        val t = createT
        val updatedT = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val upsertResult = store
            .upsert(id)(t)(_ => updatedT)

          upsertResult.right.value shouldBe Identified(Version(id, 1), updatedT)
          store.get(Version(id, 1)).right.value shouldBe Identified(Version(id, 1), updatedT)
        }
      }

      it("writes when an id does not exist") {
        withVersionedStore() { store =>
          val id = createIdent
          val t = createT
          val otherT = createT

          val upsertResult = store
            .upsert(id)(t)(_ => otherT)

          upsertResult.right.value shouldBe Identified(Version(id, 0), t)
          store.get(Version(id, 0)).right.value shouldBe Identified(Version(id, 0), t)
        }
      }
    }

    describe("update") {
      it("updates an existing id") {
        val id = createIdent

        val t = createT
        val updatedT = createT

        withVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val update = store
            .update(id)(_ => updatedT)

          update.right.value shouldBe Identified(Version(id, 1), updatedT)
          store.get(Version(id, 1)).right.value shouldBe Identified(Version(id, 1), updatedT)
        }
      }

      it("refuses to write when an id does not exist") {
        withVersionedStore() { store =>
          val id = createIdent
          val t = createT

          val update = store
            .update(id)(_ => t)

          update.left.value shouldBe a[UpdateNoSourceError]
        }
      }

      it("fails if the underlying dao fails to write") {
        val id = createIdent
        val t = createT

        val version = Version(id, 0)

        withFailingPutVersionedStore(initialEntries = Map(version -> t)) { store =>
          val update = store
            .update(id)(_ => t)

          update.left.value shouldBe a[UpdateWriteError]
        }
      }

      it("fails if the underlying dao fails to read") {
        val id = createIdent

        val t = createT

        withFailingGetVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val update = store.update(id)(_ => t)

          update.left.value shouldBe a[UpdateReadError]
        }
      }
    }

    it("is internally consistent") {
      val id = createIdent

      val t0 = createT
      val t1 = createT
      val t2 = createT
      val t3 = createT
      val t4 = createT

      withVersionedStore() { store =>
        store.init(id)(t0) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 0), t0)
        store.get(Version(id, 0)).right.value shouldBe Identified(Version(id, 0), t0)

        store.putLatest(id)(t1) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 1), t1)

        store.put(Version(id, 2))(t2) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 2), t2)

        store.upsert(id)(t0)(_ => t3) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 3), t3)

        store.update(id)(_ => t4) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 4), t4)
      }
    }
  }
}