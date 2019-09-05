package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.MemoryStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class VersionedStoreRaceConditionsTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with EitherValues
    with IntegrationPatience
    with Logging {

  // Regression tests for https://github.com/wellcometrust/platform/issues/3876
  //
  // We saw an issue with an implementation of putLatest() where it
  // could throw a VersionAlreadyExistsError.  It takes a fairly precise
  // set of timing conditions.
  //
  // Specifically: at the time of the issue, the flow for putLatest() was:
  //
  //    putLatest(id)(t)
  //      -> nextVersionFor(id)         // get the version to store
  //         = store.max(id) + 1        // increment that for the new version
  //      -> put(Version(id, v), t)     // call put() with the result of nextVersionFor()
  //        -> store.max(id)            // what's the current version in the store?
  //
  // It then checks the current version, and returns VersionAlreadyExistsError or
  // HigherVersionExistsError if the current version is equal to/ahead of
  // the version you're trying to store.
  //
  // If two processes call putLatest() simultaneously, the calls to store.max()
  // could cause an issue.  If you get the new version, then somebody stores that
  // version before you're done, you get a VersionError even though putLatest()
  // is meant to handle that for you.
  //
  // It should return a StoreWriteError with RetryableError.
  //
  describe("handles races in putLatest") {
    it("handles a VersionAlreadyExistsError") {
      // Desired sequence of events:
      //
      //      putLatest("trouble")        putLatest("tantrum")
      //
      //      - assign version v = 0
      //                                  - assign version v = 0
      //                                  - check version
      //                                  - store version v = 0
      //      - check version
      //        # ERROR! #
      //
      // This should get a VersionAlreadyExistsError error from the underlying store.
      // Calling putLatest() twice needs four calls to max().
      //
      // In this test, we want to block allow the second max() in one call
      // to putLatest() until the other putLatest() has completed.
      var actualMaxCalls = 0
      var allowedMaxCalls = 3

      val store = new MemoryStore[Version[String, Int], String](initialEntries = Map.empty) with MemoryMaxima[String, String] {
        override def max(id: String): Either[MaximaError, Int] = {
          debug(s"Calling max(id = $id)")
          while (allowedMaxCalls <= actualMaxCalls) {
            debug(s"max: actualMaxCalls = $actualMaxCalls, allowedMaxCalls = $allowedMaxCalls")
            debug(s"Waiting for max() to be available")
            Thread.sleep(10)
          }
          actualMaxCalls += 1
          super.max(id)
        }

        override def put(id: Version[String, Int])(t: String): Either[WriteError, Identified[Version[String, Int], String]] = {
          debug(s"Calling put(id = $id, t = $t)")
          val result = super.put(id)(t)

          allowedMaxCalls += 1
          debug(s"put: actualMaxCalls = $actualMaxCalls, allowedMaxCalls = $allowedMaxCalls")

          result
        }
      }

      val versionedStore = new VersionedStore[String, Int, String](store)

      val future: Future[Seq[versionedStore.WriteEither]] = Future.sequence(Seq(
        Future { versionedStore.putLatest(id = "1")(t = "trouble") },
        Future { versionedStore.putLatest(id = "1")(t = "tantrum") }
      ))

      whenReady(future) { result =>
        debug(s"result = $result")
        result.count { _.isRight } shouldBe 1

        val errors = result.collect { case Left(err) => err }

        val writeError = errors.collect { case e: StoreWriteError => e }.head
        writeError shouldBe a[RetryableError]
        writeError.e.getMessage shouldBe "Another process wrote to id=1 simultaneously"
      }
    }

    it("handles a HigherVersionAlreadyExistsError") {
      // Desired sequence of events:
      //
      //      putLatest("tapir")          putLatest("turtle")       putLatest("toucan")
      //
      //      - assign version v = 0
      //                                  - assign version v = 0
      //                                  - check version
      //                                  - store version v = 0
      //                                                            - assign version v = 1
      //                                                            - check version
      //                                                            - assign version v = 2
      //      - check version
      //        # ERROR! #
      //
      // This should get a HigherVersionAlreadyExists error from the underlying store.
      //
      // So we want to block the second call to max() in putLatest("tapir") until
      // both the other put calls have completed.

      val workingStore = new MemoryStore[Version[String, Int], String](initialEntries = Map.empty)
        with MemoryMaxima[String, String]

      var actualMaxCalls = 0
      var allowedMaxCalls = 1

      // The two stores share their entries, but we want to gate the calls
      // to max() in this store.
      val stoppingStore = new Store[Version[String, Int], String] with Maxima[String, Int] {
        override def max(id: String): Either[MaximaError, Int] = {
          debug(s"Calling max(id = $id)")
          while (allowedMaxCalls <= actualMaxCalls) {
            debug(s"max: actualMaxCalls = $actualMaxCalls, allowedMaxCalls = $allowedMaxCalls")
            debug(s"Waiting for max() to be available")
            Thread.sleep(10)
          }
          actualMaxCalls += 1

          workingStore.max(id)
        }

        override def get(id: Version[String, Int]): Either[ReadError, Identified[Version[String, Int], String]] =
          workingStore.get(id)

        override def put(id: Version[String, Int])(t: String): WriteEither =
          workingStore.put(id)(t)
      }

      val workingVersionedStore = new VersionedStore[String, Int, String](workingStore)
      val stoppingVersionedStore = new VersionedStore[String, Int, String](stoppingStore)

      // Call putLatest() in the stopping store.  This will get stuck waiting max().
      val future = Future { stoppingVersionedStore.putLatest(id = "1")(t = "tapir") }

      // Wait for a bit, so it gets stuck.
      Thread.sleep(250)

      // Now call putLatest() in the working store, so incrementing the version.
      workingVersionedStore.putLatest(id = "1")(t = "turtle")
      workingVersionedStore.putLatest(id = "1")(t = "toucan")

      // Once those both succeed, we can allow the second max() call to proceed.
      allowedMaxCalls += 1

      whenReady(future) { result =>
        debug(s"result = $result")

        val writeError = result.left.value
        writeError shouldBe a[StoreWriteError]
        writeError shouldBe a[RetryableError]
        writeError.e.getMessage shouldBe "Another process wrote to id=1 simultaneously"
      }
    }
  }
}
