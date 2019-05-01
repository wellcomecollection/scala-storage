package uk.ac.wellcome.storage.fixtures

import java.util.UUID

import org.scalatest.{Assertion, EitherValues, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{LockDao, UnlockFailure}
import uk.ac.wellcome.storage.locking._

import scala.util.Try

trait LockingServiceFixtures
  extends LockDaoFixtures
    with EitherValues
    with TryValues
    with Matchers {

  type LockDaoStub = LockDao[String, UUID]
  type ResultF = Try[Either[FailedLockingServiceOp, String]]
  type LockingServiceStub =
    LockingService[String, Try, LockDaoStub]

  def withLockingService[R](maybeLockDao: Option[LockDaoStub] = None)(testWith: TestWith[LockingServiceStub, R]): R =
    testWith(
      new LockingService[String, Try, LockDaoStub] {
      type UnlockFail = UnlockFailure[String]

      override implicit val lockDao = maybeLockDao.getOrElse(createInMemoryLockDao)
      override protected def createContextId: UUID =
        UUID.randomUUID()
    })

  def successfulRightOf(result: ResultF): String =
    result
      .success.value
      .right.value

  def successfulLeftOf(result: ResultF): FailedLockingServiceOp =
    result
      .success.value
      .left.value

  def assertLockSuccess(result: ResultF): Assertion = {
    debug(s"Got $result")
    successfulRightOf(result) shouldBe expectedResult
  }

  def assertFailedLock(result: ResultF, lockIds: Set[String]): Assertion = {
    debug(s"Got $result, with $lockIds")
    val failedLock = successfulLeftOf(result)
      .asInstanceOf[FailedLock[String, String]]

    failedLock.lockFailures shouldBe a[Set[_]]
    failedLock.lockFailures
      .map { _.id } should contain theSameElementsAs lockIds
  }

  def assertFailedProcess(result: ResultF, e: Throwable): Assertion = {
    val failedLock = successfulLeftOf(result)
      .asInstanceOf[FailedProcess[String]]

    failedLock.e shouldBe e
  }

  private val randomString = UUID.randomUUID().toString

  val expectedResult: String = randomString
  val expectedError: Error = new Error(randomString)

  def f = Try { expectedResult }
  def fError = Try { throw expectedError }
}
