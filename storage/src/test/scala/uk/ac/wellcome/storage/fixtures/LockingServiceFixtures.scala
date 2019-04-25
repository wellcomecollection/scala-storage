package uk.ac.wellcome.storage.fixtures

import java.util.UUID

import org.scalatest.{Assertion, EitherValues, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.locking._

import scala.util.Try

trait LockingServiceFixtures
  extends LockDaoFixtures
    with EitherValues
    with TryValues
    with Matchers {

  type InMemoryLockDao = LockDao[String, String, Unit]
  type ResultF = Try[Either[FailedLockingServiceOp, String]]
  type InMemoryTryLockingService = LockingService[
    String, String, String, Throwable, List, Try, InMemoryLockDao
    ]

  def withLockingService[R](
                             testWith: TestWith[InMemoryTryLockingService, R]): R =
    testWith(new LockingService[
      String, String, String, Throwable, List, Try, InMemoryLockDao
      ] {
      override implicit val lockDao = createInMemoryLockDao
      override protected def createContextId: String =
        UUID.randomUUID().toString
    })

  def successfulRightOf(result: ResultF): String =
    result
      .success.value
      .right.value

  def successfulLeftOf(result: ResultF): FailedLockingServiceOp =
    result
      .success.value
      .left.value

  def assertLockSuccess(result: ResultF): Assertion =
    successfulRightOf(result) shouldBe expectedResult

  def assertFailedLock(result: ResultF, lockIds: List[String]): Assertion = {
    val failedLock = successfulLeftOf(result)
      .asInstanceOf[FailedLock[String, String]]

    failedLock.lockFailures shouldBe a[List[_]]
    failedLock.lockFailures
      .map { _.id } should contain theSameElementsAs lockIds
  }

  def assertFailedProcess(result: ResultF, e: Throwable): Assertion = {
    val failedLock = successfulLeftOf(result)
      .asInstanceOf[FailedProcess[String]]

    failedLock.e shouldBe e
  }

  val randomString = UUID.randomUUID().toString

  val expectedResult = randomString
  val expectedError = new Error(randomString)

  def f = Try { expectedResult }
  def fError = Try { throw expectedError }
}
