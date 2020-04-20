package uk.ac.wellcome.storage

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RetryOpsTest extends AnyFunSpec with Matchers with EitherValues {
  import RetryOps._

  it("if N=1 is a Right[…], it succeeds") {
    var callCount = 0

    def canOnlyBeCalledOnce(s: String): Either[Throwable, String] = {
      callCount += 1

      if (callCount == 1) {
        Right(s.toUpperCase())
      } else {
        throw new Throwable("BOOM!")
      }
    }

    val retryableFunction = (canOnlyBeCalledOnce _).retry(maxAttempts = 3)

    val result = retryableFunction("hello")
    result.right.value shouldBe "HELLO"
  }

  it("if N=1–5 are retryable and N=6 is a Right[…], it succeeds") {
    val maxAttempts = 6

    var callCount = 0

    def countCalls(s: String): Either[Throwable, Any] = {
      callCount += 1

      if (callCount > maxAttempts - 1) {
        Right(s)
      } else {
        Left(new Throwable("BOOM!") with RetryableError)
      }
    }

    val retryableFunction = (countCalls _).retry(maxAttempts)
    retryableFunction("hello").right.value shouldBe "hello"
  }

  it("if N=1 is retryable and N=2 is a Left[…], it fails") {
    var callCount = 0
    val err = new Throwable("BOOM!")

    def failsAfterFirstCall(s: String): Either[Throwable, Int] = {
      callCount += 1

      if (callCount == 1) {
        Left(new Throwable("BOOM!") with RetryableError)
      } else {
        Left(err)
      }
    }

    val retryableFunction = (failsAfterFirstCall _).retry(maxAttempts = 3)

    retryableFunction("hello").left.value shouldBe err
  }

  it("if N=1 to maxAttempts are all retryable, it fails") {
    val maxAttempts = 5

    val err = new Throwable("BOOM!") with RetryableError

    def alwaysFails(s: String): Either[Throwable, Any] =
      Left(err)

    val retryableFunction = (alwaysFails _).retry(maxAttempts)
    retryableFunction("hello").left.value shouldBe err
  }

  it("makes one attempt by default") {
    var callCount = 0
    val err = new Throwable("BOOM!") with RetryableError

    def countCalls(s: String): Either[Throwable, Any] = {
      callCount += 1
      Left(err)
    }

    val retryableFunction = (countCalls _).retry()
    retryableFunction("hello").left.value shouldBe err

    callCount shouldBe 1
  }
}
