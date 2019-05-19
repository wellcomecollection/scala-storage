package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}

class BetterVHSTest extends FunSpec with Matchers {
  it("is consistent with itself") {}

  describe("storing a new record") {
    it("stores the object in the store") {}

    it("stores the metadata in the dao") {}

    it("stores an object with the id as a prefix") {}
  }

  describe("updating an existing record") {
    it("stores the new object and metadata") {}

    it("updates if only the object has changed") {}

    it("updates if only the metadata has changed") {}

    it("skips the update if nothing has changed") {}
  }

  describe("errors when storing the record") {
    it("fails if the object store has an error") {}

    it("fails if the dao has an error") {}
  }

  describe("getting a record") {
    it("finds an existing object") {}

    it("returns None if the id refers to a non-existent object") {}
  }

  describe("errors when getting the record") {
    it("fails if the dao refers to a missing object in the store") {}

    it("fails if the object store has an error") {}

    it("fails if the dao has an error") {}
  }
}
