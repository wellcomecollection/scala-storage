package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage.{Identified, Version}

trait VersionedStoreWithoutOverwriteTestCases[Id, T, VersionedStoreContext]
    extends VersionedStoreWithOverwriteTestCases[Id, T, VersionedStoreContext] {
  describe("it behaves as a VersionedStore") {
    describe("get") {
      describe("without overwrite") {
        it("gets a stored record at a specified version") {
          val id = createIdent

          val t1 = createT
          val t2 = createT

          withVersionedStoreImpl(
            initialEntries = Map(Version(id, 1) -> t1, Version(id, 2) -> t2)
          ) { store =>
            val result = for {
              result <- store.get(Version(id, 1))
            } yield result

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

          withVersionedStoreImpl(
            initialEntries = Map(
              Version(id, 1) -> t1,
              Version(id, 2) -> t2,
              Version(id, 3) -> t3,
              Version(id, 4) -> t4,
              Version(id, 5) -> t5
            )
          ) { store =>
            store.get(Version(id, 1)).right.value shouldBe Identified(
              Version(id, 1),
              t1)
            store.get(Version(id, 2)).right.value shouldBe Identified(
              Version(id, 2),
              t2)
            store.get(Version(id, 3)).right.value shouldBe Identified(
              Version(id, 3),
              t3)
            store.get(Version(id, 4)).right.value shouldBe Identified(
              Version(id, 4),
              t4)
            store.get(Version(id, 5)).right.value shouldBe Identified(
              Version(id, 5),
              t5)
          }
        }
      }
    }
  }
}
