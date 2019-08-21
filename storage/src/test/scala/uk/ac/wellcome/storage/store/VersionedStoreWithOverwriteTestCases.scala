package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.fixtures.VersionedStoreFixtures

trait VersionedStoreWithOverwriteTestCases[Id, T, VersionedStoreContext]
    extends FunSpec
    with Matchers
    with EitherValues
    with Logging
    with VersionedStoreFixtures[Id, Int, T, VersionedStoreContext]
    with StoreWithoutOverwritesTestCases[
      Version[Id, Int],
      T,
      String,
      VersionedStoreContext] {

  def createIdent: Id
  def createT: T

  def withVersionedStoreImpl[R](initialEntries: Entries = Map.empty)(
    testWith: TestWith[VersionedStoreImpl, R]): R

  def withFailingGetVersionedStore[R](initialEntries: Entries = Map.empty)(
    testWith: TestWith[VersionedStoreImpl, R]): R
  def withFailingPutVersionedStore[R](initialEntries: Entries = Map.empty)(
    testWith: TestWith[VersionedStoreImpl, R]): R

  describe("it behaves as a VersionedStore") {
    describe("init") {
      it("stores a new record at the starting version") {
        withVersionedStoreImpl() { versionedStore =>
          val id = createIdent
          val t = createT

          val result = versionedStore.init(id)(t)
          val value = result.right.value

          value.identifiedT shouldBe t
          value.id shouldBe Version(id, 0)
        }
      }

      it("does not store a new record if one exists") {
        val id = createIdent

        val t1 = createT
        val t2 = createT

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t1)
        ) { store =>
          val result = store.init(id)(t2)
          val err = result.left.value

          err shouldBe a[VersionAlreadyExistsError]
        }
      }

      it("fails if the underlying store fails to put") {
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
        withVersionedStoreImpl() { store =>
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

        withVersionedStoreImpl(
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

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t1)
        ) { store =>
          val result = store.put(Version(id, 1))(t2)
          val err = result.right.value

          err.identifiedT shouldEqual t2
          err.id shouldEqual Version(id, 1)
        }
      }

      describe(
        "increments version monotonically where there are gaps in the version sequence") {
        it("version is specified") {
          val id = createIdent

          val t0 = createT
          val t2 = createT

          withVersionedStoreImpl(
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

          withVersionedStoreImpl(
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

        withVersionedStoreImpl(
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

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          store
            .put(Version(id, 0))(t)
            .left
            .value shouldBe a[VersionAlreadyExistsError]
        }
      }

      it("fails if the underlying store fails to put") {
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

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val result = store.getLatest(id)
          val value = result.right.value

          value shouldBe Identified(Version(id, 0), t)
        }
      }

      it("fails when getting a non-existent version on an id") {
        val id = createIdent
        val t = createT

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          store.get(Version(id, 0)).right.value shouldBe Identified(
            Version(id, 0),
            t)
          store.get(Version(id, 1)).left.value shouldBe a[NoVersionExistsError]
        }
      }

      it("fails when getting a non-existent id and version pair") {
        withVersionedStoreImpl() { store =>
          val id = createIdent

          store.get(Version(id, 1)).left.value shouldBe a[NoVersionExistsError]
        }
      }

      it("fails when getting a non-existent id") {
        withVersionedStoreImpl() { store =>
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

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val f = (_: T) => Right(updatedT)

          val upsertResult = store.upsert(id)(t)(f)

          upsertResult.right.value shouldBe Identified(
            Version(id, 1),
            updatedT)
          store.get(Version(id, 1)).right.value shouldBe Identified(
            Version(id, 1),
            updatedT)
        }
      }

      it("writes when an id does not exist") {
        withVersionedStoreImpl() { store =>
          val id = createIdent
          val t = createT
          val otherT = createT

          val f = (_: T) => Right(otherT)

          val upsertResult = store.upsert(id)(t)(f)

          upsertResult.right.value shouldBe Identified(Version(id, 0), t)
          store.get(Version(id, 0)).right.value shouldBe Identified(
            Version(id, 0),
            t)
        }
      }
    }

    describe("update") {
      it("updates an existing id") {
        val id = createIdent

        val t = createT
        val updatedT = createT

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val f = (_: T) => Right(updatedT)

          val update = store
            .update(id)(f)

          update.right.value shouldBe Identified(Version(id, 1), updatedT)
          store.get(Version(id, 1)).right.value shouldBe Identified(
            Version(id, 1),
            updatedT)
        }
      }

      it("refuses to write when an id does not exist") {
        withVersionedStoreImpl() { store =>
          val id = createIdent
          val t = createT

          val f = (_: T) => Right(t)

          val update = store
            .update(id)(f)

          update.left.value shouldBe a[UpdateNoSourceError]
        }
      }

      it("fails when an update function returns a Left[UpdateNotApplied]") {
        val id = createIdent
        val t = createT

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val err = new Throwable("BOOM!")

          val f = (_: T) => Left(UpdateNotApplied(err))

          val update = store
            .update(id)(f)

          val result = update.left.value
          result shouldBe a[UpdateNotApplied]
          result.e shouldBe err
        }
      }

      it("fails when an update function throws") {
        val id = createIdent
        val t = createT

        withVersionedStoreImpl(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val err = new Throwable("BOOM!")
          val f = (_: T) => throw err

          val update = store
            .update(id)(f)

          val result = update.left.value
          result shouldBe a[UpdateUnexpectedError]
          result.e shouldBe err
        }
      }

      it("fails if the underlying store fails to write") {
        val id = createIdent
        val t = createT

        val version = Version(id, 0)

        withFailingPutVersionedStore(initialEntries = Map(version -> t)) {
          store =>
            val f = (_: T) => Right(t)

            val update = store
              .update(id)(f)

            update.left.value shouldBe a[UpdateWriteError]
        }
      }

      it("fails if the underlying store fails to read") {
        val id = createIdent

        val t = createT

        withFailingGetVersionedStore(
          initialEntries = Map(Version(id, 0) -> t)
        ) { store =>
          val f = (_: T) => Right(t)

          val update = store
            .update(id)(f)

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

      withVersionedStoreImpl() { store =>
        store.init(id)(t0) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 0), t0)
        store.get(Version(id, 0)).right.value shouldBe Identified(
          Version(id, 0),
          t0)

        store.putLatest(id)(t1) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 1), t1)

        store.put(Version(id, 2))(t2) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 2), t2)

        val f1 = (_: T) => Right(t3)
        val f2 = (_: T) => Right(t4)

        store.upsert(id)(t0)(f1) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 3), t3)

        store.update(id)(f2) shouldBe a[Right[_, _]]
        store.getLatest(id).right.value shouldBe Identified(Version(id, 4), t4)
      }
    }
  }
}
