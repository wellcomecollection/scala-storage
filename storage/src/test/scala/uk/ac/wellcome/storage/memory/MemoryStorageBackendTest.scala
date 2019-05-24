package uk.ac.wellcome.storage.memory

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.streaming.CodecInstances._
import uk.ac.wellcome.storage._

class MemoryStorageBackendTest extends FunSpec with Matchers with EitherValues {
  it("behaves correctly") {
    val backend = new MemoryStorageBackend()

    val loc1 = ObjectLocation("documents", "1.txt")
    val doc1 = "hello world"
    put(backend, location = loc1, s = doc1) shouldBe Right(())

    val loc2 = ObjectLocation("documents", "2.txt")
    val doc2 = "howdy friends"
    put(backend, location = loc2, s = doc2) shouldBe Right(())

    get(backend, loc1) shouldBe Right(doc1)
    get(backend, loc2) shouldBe Right(doc2)

    val loc3 = ObjectLocation("documents", "3.txt")
    get(backend, loc3).left.value shouldBe a[BackendReadError]

    put(backend, location = loc1, s = doc2)
    get(backend, loc1) shouldBe Right(doc2)
  }

  private def get(backend: MemoryStorageBackend, location: ObjectLocation): Either[ReadError, String] =
    backend.get(location).flatMap { stringCodec.fromStream }

  private def put(backend: MemoryStorageBackend, location: ObjectLocation, s: String): Either[WriteError, Unit] =
    stringCodec.toStream(s).flatMap { inputStream =>
      backend.put(
        location = location,
        inputStream = inputStream,
        metadata = Map.empty
      )
    }
}
