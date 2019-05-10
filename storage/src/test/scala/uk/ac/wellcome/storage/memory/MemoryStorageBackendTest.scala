package uk.ac.wellcome.storage.memory

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.StreamHelpers

import scala.util.{Failure, Success, Try}

class MemoryStorageBackendTest extends FunSpec with Matchers with StreamHelpers {
  it("behaves correctly") {
    val backend = new MemoryStorageBackend()

    val loc1 = ObjectLocation("documents", "1.txt")
    val doc1 = "hello world"
    put(backend, location = loc1, s = doc1) shouldBe Success(())

    val loc2 = ObjectLocation("documents", "2.txt")
    val doc2 = "howdy friends"
    put(backend, location = loc2, s = doc2) shouldBe Success(())

    get(backend, loc1) shouldBe Success(doc1)
    get(backend, loc2) shouldBe Success(doc2)

    val loc3 = ObjectLocation("documents", "3.txt")
    get(backend, loc3) shouldBe a[Failure[_]]

    put(backend, location = loc1, s = doc2)
    get(backend, loc1) shouldBe Success(doc2)
  }

  private def get(backend: MemoryStorageBackend, location: ObjectLocation): Try[String] =
    backend.get(location).map { fromStream }

  private def put(backend: MemoryStorageBackend, location: ObjectLocation, s: String): Try[Unit] =
    backend.put(
      location = location,
      input = toStream(s),
      metadata = Map.empty
    )
}
