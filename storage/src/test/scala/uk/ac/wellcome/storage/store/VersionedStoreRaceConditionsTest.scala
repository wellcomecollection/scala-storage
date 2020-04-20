package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.MemoryStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class VersionedStoreRaceConditionsTest
  extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with EitherValues
    with IntegrationPatience
    with Logging {

  class StepControlledStore() {
    private val workingStore = new MemoryStore[Version[String, Int], String](initialEntries = Map.empty)
      with MemoryMaxima[String, String]

    private var actualMaxCalls = 0

    var allowedMaxCalls = 0

    // The two stores share their entries, but we want to gate the calls
    // to max() in this store.
    private val stoppingStore = new Store[Version[String, Int], String] with Maxima[String, Int] {
      override def max(id: String): Either[MaximaError, Int] = {
        debug(s"Calling max(id = $id)")
        debug(s"max: actualMaxCalls = $actualMaxCalls, allowedMaxCalls = $allowedMaxCalls")
        while (allowedMaxCalls <= actualMaxCalls) {
          debug(s"Waiting for max() to be available")
          Thread.sleep(10)
        }
        actualMaxCalls += 1

        workingStore.max(id)
      }

      override def get(id: Version[String, Int]): Either[ReadError, Identified[Version[String, Int], String]] = {
        debug(s"Calling get(id = $id)")
        workingStore.get(id)
      }

      override def put(id: Version[String, Int])(t: String): WriteEither = {
        debug(s"Calling put(id = $id)(t = $t)")
        workingStore.put(id)(t)
      }
    }

    def getWorkingStore: VersionedStore[String, Int, String] =
      new VersionedStore[String, Int, String](workingStore)

    def getStoppingStore: VersionedStore[String, Int, String] =
      new VersionedStore[String, Int, String](stoppingStore)
  }

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
      val stores = new StepControlledStore()
      stores.allowedMaxCalls = 1

      stores.allowedMaxCalls shouldBe 1

      val workingVersionedStore = stores.getWorkingStore
      val stoppingVersionedStore = stores.getStoppingStore

      // Call putLatest() in the stopping store.  This will get stuck waiting max().
      val future = Future { stoppingVersionedStore.putLatest(id = "1")(t = "trouble") }

      // Wait for a bit, so it gets stuck.
      Thread.sleep(250)

      // Now call putLatest() in the working store, so incrementing the version.
      workingVersionedStore.putLatest(id = "1")(t = "tantrum")

      // Once those both succeed, we can allow the second max() call to proceed.
      stores.allowedMaxCalls += 1

      whenReady(future) { result =>
        debug(s"result = $result")

        val writeError = result.left.value
        writeError shouldBe a[StoreWriteError]
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
      val stores = new StepControlledStore()
      stores.allowedMaxCalls = 1

      val workingVersionedStore = stores.getWorkingStore
      val stoppingVersionedStore = stores.getStoppingStore

      // Call putLatest() in the stopping store.  This will get stuck waiting max().
      val future = Future { stoppingVersionedStore.putLatest(id = "1")(t = "tapir") }

      // Wait for a bit, so it gets stuck.
      Thread.sleep(250)

      // Now call putLatest() in the working store, so incrementing the version.
      workingVersionedStore.putLatest(id = "1")(t = "turtle")
      workingVersionedStore.putLatest(id = "1")(t = "toucan")

      // Once those both succeed, we can allow the second max() call to proceed.
      stores.allowedMaxCalls += 1

      whenReady(future) { result =>
        debug(s"result = $result")

        val writeError = result.left.value
        writeError shouldBe a[StoreWriteError]
        writeError shouldBe a[RetryableError]
        writeError.e.getMessage shouldBe "Another process wrote to id=1 simultaneously"
      }
    }
  }

  // We saw an issue in update() where it could throw version-related error.
  // Again, requires precise set of timing conditions.
  //
  // When we first hit the bug, the flow for update() was:
  //
  //    update(id)(f)
  //      -> getLatest(id)
  //         newValue = f(…)
  //      -> put(Version(id, v + 1), newValue)
  //
  // The put will check the current version a second time, so if somebody
  // writes before the putLatest succeeds, the call will fail.
  //
  describe("handles races in update() and upsert()") {
    // Desired sequence of events:
    //
    //      update("1")(upperCase)      update("1")(lowercase)
    //
    //      - read existing v = 0       - read existing v = 0
    //      - assign version v = 1      - assign version v = 1
    //      - call put()
    //      - store version v = 1
    //                                  - call put()
    //                                  - check version
    //                                    # ERROR! #
    //      - check version
    //        # ERROR! #
    //
    // This should get a VersionAlreadyExistsError error from the underlying store.
    // Calling update() twice needs four calls to max().
    //
    // In this test, we want to block allow the second max() in one call
    // to update() until the other update() has completed.
    describe("handling a VersionAlreadyExistsError") {
      it("in update()") {
        val stores = new StepControlledStore()
        stores.allowedMaxCalls = 1

        val workingVersionedStore = stores.getWorkingStore
        val stoppingVersionedStore = stores.getStoppingStore

        workingVersionedStore.init(id = "1")(t = "Snekks")

        // Call update() in the stopping store.  This will get stuck waiting max().
        val future = Future {
          stoppingVersionedStore.update(id = "1")(t => Right(t.toLowerCase))
        }

        Thread.sleep(250)

        // Now call update() in the working store, so incrementing the version.
        workingVersionedStore.update(id = "1")(t => Right(t.toUpperCase)) shouldBe a[Right[_, _]]

        // Once those both succeed, we can allow the second max() call to proceed.
        stores.allowedMaxCalls += 1

        whenReady(future) { result =>
          debug(s"result = $result")

          val updateError = result.left.value
          updateError shouldBe a[UpdateWriteError]
          updateError.asInstanceOf[UpdateWriteError].err shouldBe a[VersionAlreadyExistsError]
          updateError shouldBe a[RetryableError]
        }
      }

      it("in upsert()") {
        val stores = new StepControlledStore()
        stores.allowedMaxCalls = 1

        val workingVersionedStore = stores.getWorkingStore
        val stoppingVersionedStore = stores.getStoppingStore

        // Call upsert() in the stopping store.  This will get stuck waiting for max().
        val future = Future {
          stoppingVersionedStore.upsert(id = "1")(t = "Tarantula")(t => Right(t.toLowerCase))
        }

        Thread.sleep(250)

        // Now call upsert() in the working store, so incrementing the version.
        workingVersionedStore.upsert(id = "1")(t = "Terrapin")(t => Right(t.toUpperCase)) shouldBe a[Right[_, _]]

        // Once those both succeed, we can allow the second max() call to proceed.
        stores.allowedMaxCalls += 1

        whenReady(future) { result =>
          debug(s"result = $result")

          val updateError = result.left.value
          updateError shouldBe a[UpdateWriteError]
          updateError.asInstanceOf[UpdateWriteError].err shouldBe a[VersionAlreadyExistsError]
          updateError shouldBe a[RetryableError]
        }
      }
    }
  }

  describe("handling a HigherVersionAlreadyExistsError") {
    it("in update()") {
      val stores = new StepControlledStore()
      stores.allowedMaxCalls = 1

      val workingVersionedStore = stores.getWorkingStore
      val stoppingVersionedStore = stores.getStoppingStore

      workingVersionedStore.init(id = "1")(t = "Snekks")

      // Call update() in the stopping store.  This will get stuck waiting max().
      val future = Future {
        stoppingVersionedStore.update(id = "1")(t => Right(t.toLowerCase))
      }

      Thread.sleep(250)

      workingVersionedStore.update(id = "1")(t => Right(t.toUpperCase)) shouldBe a[Right[_, _]]
      workingVersionedStore.update(id = "1")(t => Right(t + t)) shouldBe a[Right[_, _]]

      // Once those both succeed, we can allow the second max() call to proceed.
      stores.allowedMaxCalls += 1

      whenReady(future) { result =>
        debug(s"result = $result")

        val updateError = result.left.value
        updateError shouldBe a[UpdateWriteError]
        updateError.asInstanceOf[UpdateWriteError].err shouldBe a[HigherVersionExistsError]
        updateError shouldBe a[RetryableError]
      }
    }

    it("in upsert()") {
      val stores = new StepControlledStore()
      stores.allowedMaxCalls = 1

      val workingVersionedStore = stores.getWorkingStore
      val stoppingVersionedStore = stores.getStoppingStore

      // Call upsert() in the stopping store.  This will get stuck waiting for max().
      val future = Future {
        stoppingVersionedStore.upsert(id = "1")(t = "Tarantula")(t => Right(t.toLowerCase))
      }

      Thread.sleep(250)

      // Now call upsert() in the working store, so incrementing the version.
      workingVersionedStore.upsert(id = "1")(t = "Terrapin")(t => Right(t.toUpperCase)) shouldBe a[Right[_, _]]
      workingVersionedStore.upsert(id = "1")(t = "Terrapin")(t => Right(t + t)) shouldBe a[Right[_, _]]

      // Once those both succeed, we can allow the second max() call to proceed.
      stores.allowedMaxCalls += 1

      whenReady(future) { result =>
        debug(s"result = $result")

        val updateError = result.left.value
        updateError shouldBe a[UpdateWriteError]
        updateError.asInstanceOf[UpdateWriteError].err shouldBe a[HigherVersionExistsError]
        updateError shouldBe a[RetryableError]
      }
    }
  }
}
