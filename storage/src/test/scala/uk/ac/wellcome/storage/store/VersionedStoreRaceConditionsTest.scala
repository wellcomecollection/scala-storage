package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.MemoryStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class VersionedStoreRaceConditionsTest
  extends FunSpec
    with Matchers
    with ScalaFutures
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
      // In this case, we want to block allow the second max() in one call
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
        errors.count { _.isInstanceOf[StoreWriteError] } shouldBe 1
        errors.count { _.isInstanceOf[RetryableError] } shouldBe 1
      }
    }


  }
}
