package uk.ac.wellcome.storage.locking

import java.time.Instant

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.query.Condition
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo, Table}
import grizzled.slf4j.Logging

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class DynamoRowLockDao(
  dynamoDbClient: AmazonDynamoDB,
  rowLockDaoConfig: DynamoRowLockDaoConfig)(implicit ec: ExecutionContext)
    extends Logging {

  implicit val instantLongFormat: AnyRef with DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](
      Instant.ofEpochSecond
    )(
      _.getEpochSecond
    )

  private val table = Table[RowLock](rowLockDaoConfig.dynamoConfig.table)
  private val index = rowLockDaoConfig.dynamoConfig.index

  private def getExpiry: (Instant, Instant) = {
    val created = Instant.now()
    val expires = created.plus(rowLockDaoConfig.duration)

    (created, expires)
  }

  def lockRow(id: Identifier, contextId: String): Future[RowLock] =
    Future {
      val (created, expires) = getExpiry
      val rowLock = RowLock(id.id, contextId, created, expires)
      trace(s"Locking $rowLock")

      val scanamoOps = table
        .given(not(attributeExists('id)) or Condition(
          'expires < rowLock.created.getEpochSecond))
        .put(rowLock)
      val result = Scanamo.exec(dynamoDbClient)(scanamoOps)
      trace(s"Got $result for $rowLock")

      result match {
        case Right(_) => rowLock
        case Left(error) =>
          debug(s"Failed to lock $rowLock $error")
          throw FailedLockException(s"Failed to lock $id", error)
      }
    }.recover {
      case exception: Exception =>
        val errorMsg =
          s"Problem locking row $id in context [$contextId], ${exception.getClass.getSimpleName} ${exception.getMessage}"
        debug(errorMsg)
        throw FailedLockException(errorMsg, exception)
    }

  def unlockRows(contextId: String): Future[Unit] = {
    val eventuallyDeleted = for {
      rowLockIds <- queryContextForLockIds(contextId)
      _ <- deleteRowLocks(rowLockIds.toList)
    } yield ()
    eventuallyDeleted.recover {
      case exception: Exception =>
        val errorMsg =
          s"Problem unlocking rows in context [$contextId], ${exception.getClass.getSimpleName} ${exception.getMessage}"
        debug(errorMsg)
        throw FailedUnlockException(errorMsg, exception)
    }
  }

  private def deleteRowLocks(rowLockIds: List[String]) = {
    debug(s"Unlocking rows: $rowLockIds")
    Future.sequence(rowLockIds.map { rowLockId =>
      Future {
        Scanamo.delete(dynamoDbClient)(table.name)('id -> rowLockId)
      }
    })
  }

  private def queryContextForLockIds(contextId: String): Future[Seq[String]] = {
    Future {
      debug(s"Trying to unlock context: $contextId")
      val maybeRowLocks: immutable.Seq[Either[DynamoReadError, RowLock]] =
        Scanamo.queryIndex[RowLock](dynamoDbClient)(table.name, index)(
          'contextId -> contextId)

      debug("maybeRowLocks: " + maybeRowLocks)
      maybeRowLocks.collect {
        case Right(rowLock) => rowLock.id
        case Left(error) =>
          info(s"Error $error when unlocking $contextId")
          throw FailedUnlockException(
            s"Failed to unlock [$contextId] $error",
            new RuntimeException)
      }
    }
  }
}

case class FailedLockException(message: String, cause: Throwable)
    extends Exception(message, cause)

case class FailedUnlockException(message: String, cause: Throwable)
    extends Exception(message, cause)

case class Identifier(id: String)
case class RowLock(id: String,
                   contextId: String,
                   created: Instant,
                   expires: Instant)
