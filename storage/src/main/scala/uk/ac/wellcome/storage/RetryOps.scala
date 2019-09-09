package uk.ac.wellcome.storage

import grizzled.slf4j.Logging

import scala.annotation.tailrec

/** Retry a storage operation which may return a `RetryableError`.
  *
  * It tries the function up to `maxAttempts` times.  If at any point
  * the function returns a Right[…] or a non-retryable Left[…], it
  * finishes immediately.
  *
  */
object RetryOps extends Logging {
  implicit class Retry[In, Out, OutError](f: In => Either[OutError, Out]) {

    def retry(maxAttempts: Int = 1): In => Either[OutError, Out] =
      (in: In) => retryInternal(maxAttempts)(in)

    @tailrec
    private def retryInternal(remainingAttempts: Int)(
      in: In): Either[OutError, Out] =
      f(in) match {
        case Right(out) =>
          debug(s"Success: retryable operation for in=$in succeeded")
          Right(out)

        // We can't use `Left(err: RetryableError)` because
        // the compiler needs to know that Left(err) in
        // the if branch is an instance of Error.
        //
        // Even though we know that err is both an Error and
        // a RetryableError (because it comes from f(in)),
        // the compiler can't see that.
        case Left(err) if err.isInstanceOf[RetryableError] =>
          debug(
            s"Retryable error: remaining attempts = $remainingAttempts for in=$in")
          if (remainingAttempts == 1) {
            debug(s"Retryable error: marking operation as failed with $err")
            Left(err)
          } else {
            debug(s"Retryable error: retrying operation with $err")
            retryInternal(remainingAttempts - 1)(in)
          }

        case Left(err) =>
          debug(s"Failure: retryable operation for in=$in failed with $err")
          Left(err)
      }
  }
}
