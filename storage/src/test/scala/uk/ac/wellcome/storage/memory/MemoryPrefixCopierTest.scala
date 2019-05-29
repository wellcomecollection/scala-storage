package uk.ac.wellcome.storage.memory

import java.nio.file.Paths

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.streaming.EncoderInstances.stringEncoder

class MemoryPrefixCopierTest
  extends FunSpec
  with Matchers
  with EitherValues
  with ObjectLocationGenerators {

  val srcNamespace = "namespaceSrc"
  val dstNamespace = "namespaceDst"

  def createBackend: MemoryStorageBackend = new MemoryStorageBackend()
  def createPrefixCopier(
    backend: MemoryStorageBackend = createBackend
  ): MemoryPrefixCopier = new MemoryPrefixCopier(backend)

  it("succeeds if there are no files in the prefix") {
    val prefixCopier = createPrefixCopier()

    val result = prefixCopier.copyObjects(
      srcLocationPrefix = createObjectLocation,
      dstLocationPrefix = createObjectLocation
    )

    result.right.value shouldBe PrefixCopierResult(fileCount = 0)
  }

  private def createObject(backend: MemoryStorageBackend, location: ObjectLocation): Unit =
    backend.put(
      location = location,
      inputStream = stringEncoder.toStream(randomAlphanumeric).right.value,
      metadata = Map.empty
    ).right.value

  describe("copying a single file under a prefix") {
    describe("to a key ending in /") {
      it("copies that file") {
        val backend = createBackend
        val prefixCopier = createPrefixCopier(backend)

        val srcPrefix = createObjectLocationWith(namespace = srcNamespace, key = "src")
        val src = srcPrefix.copy(key = Paths.get(srcPrefix.key,"1.txt").toString)

        createObject(backend, src)

        val dstPrefix = createObjectLocationWith(namespace = dstNamespace, key = "dst/")


        val result = prefixCopier.copyObjects(srcPrefix, dstPrefix)
        result.right.value shouldBe PrefixCopierResult(fileCount = 1)

        backend.storage.keys.toSeq should contain theSameElementsAs Seq(
          ObjectLocation(srcNamespace, "src/1.txt"),
          ObjectLocation(dstNamespace, "dst/1.txt")
        )

        val dst = dstPrefix.copy(key = Paths.get(dstPrefix.key,"1.txt").toString)
        backend.storage(src) shouldBe backend.storage(dst)
      }
    }

    describe("to a key NOT ending in /") {
      it("copies that file") {
        val backend = createBackend
        val prefixCopier = createPrefixCopier(backend)

        val srcPrefix = createObjectLocationWith(srcNamespace, key = "src")
        val src = srcPrefix.copy(key = Paths.get(srcPrefix.key,"1.txt").toString)

        createObject(backend, src)

        val dstPrefix = createObjectLocationWith(dstNamespace, key = "dst")

        val result = prefixCopier.copyObjects(srcPrefix, dstPrefix)
        result.right.value shouldBe PrefixCopierResult(fileCount = 1)

        backend.storage.keys.toSeq should contain theSameElementsAs Seq(
          ObjectLocation(srcNamespace, "src/1.txt"),
          ObjectLocation(dstNamespace, "dst/1.txt")
        )

        val dst = dstPrefix.copy(key = Paths.get(dstPrefix.key,"1.txt").toString)
        backend.storage(src) shouldBe backend.storage(dst)
      }
    }
  }

  it("copies multiple files under a prefix") {
    val backend = createBackend
    val prefixCopier = createPrefixCopier(backend)

    val srcPrefix = createObjectLocationWith(srcNamespace, key = "src/")

    val srcLocations = (1 to 5).map { i =>
      val src = srcPrefix.copy(key = srcPrefix.key + s"$i.txt")
      createObject(backend, src)
      src
    }

    val dstPrefix = createObjectLocationWith(dstNamespace, key = "dst/")

    val dstLocations = srcLocations.map { loc: ObjectLocation =>
      loc.copy(
        namespace = dstPrefix.namespace,
        key = loc.key.replace("src/", "dst/")
      )
    }

    val result = prefixCopier.copyObjects(srcPrefix, dstPrefix)
    result.right.value shouldBe PrefixCopierResult(fileCount = 5)

    backend.storage.keys.filter {_.namespace == dstNamespace} should contain theSameElementsAs dstLocations

    srcLocations.zip(dstLocations).map { case (src, dst) =>
      backend.storage(src) shouldBe backend.storage(dst)
    }
  }
}
