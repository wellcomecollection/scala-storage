package uk.ac.wellcome.storage.locking

import cats.data.EitherT
import cats.implicits._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.DynamoFormat._
import com.gu.scanamo.query.Condition
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Table}
import grizzled.slf4j.Logging

import scala.concurrent.ExecutionContext
import scala.util.Try

class DynamoLockDao(
  val client: AmazonDynamoDB,
  config: DynamoLockDaoConfig
)(implicit
  val ec: ExecutionContext,
  val df: DynamoFormat[ExpiringLock])
    extends LockDao[String, String]
    with Logging
    with ScanamoHelpers[ExpiringLock] {

  override val table = Table[ExpiringLock](config.dynamoConfig.table)
  override val index = config.dynamoConfig.index

  // Lock

  override def lock(id: String, ctxId: String): Lock = {
    val rowLock = ExpiringLock.create(id, ctxId, config.duration)

    debug(s"Locking $rowLock")

    safePutItem(lockOp(rowLock)).fold(
      e => {
        debug(s"Failed to lock $rowLock $e")
        Left(LockFailure(id, e))
      },
      result => {
        debug(s"Successful lock $result for $rowLock")
        Right(rowLock)
      }
    )
  }

  private def lockOp(lock: ExpiringLock) = {
    val lockFound = attributeExists(symbol = 'id)
    val lockNotFound = not(lockFound)

    val isExpired = Condition('expires < lock.created.getEpochSecond)

    val lockHasExpired = Condition(lockFound and isExpired)

    val lockAlreadyExists = Condition('contextId -> lock.contextId)

    val canLock =
      lockHasExpired or lockNotFound or lockAlreadyExists

    table.given(canLock).put(lock)
  }

  // Unlock

  override def unlock(ctxId: String): Unlock = {
    debug(s"Unlocking $ctxId")

    queryAndDelete(ctxId).fold(
      e => {
        warn(s"Failed to unlock $ctxId $e")
        Left(UnlockFailure(ctxId, e))
      },
      result => {
        trace(s"Unlocked $ctxId")
        Right(result)
      }
    )
  }

  private def queryAndDelete(ctxId: ContextId) =
    for {
      queryOp <- queryLocks(ctxId).toEither
      rowLocks <- queryOp
      _ <- deleteLocks(rowLocks)
    } yield ()

  private def deleteLocks(rowLocks: List[ExpiringLock]) = {
    val deleteT = EitherT(
      rowLocks.map(rowLock => toEither(delete('id -> rowLock.id))))

    val deleteErrors = deleteT.swap.collectRight
    val deletions = deleteT.collectRight

    if (deleteErrors.isEmpty) {
      Right(deletions)
    } else {
      Left(new Error(s"Deleting $rowLocks failed with $deleteErrors"))
    }
  }

  private def queryLocks(ctxId: String) = Try {
    val queryT = EitherT(queryIndex('contextId -> ctxId))

    val readErrors = queryT.swap.collectRight
    val rowLocks = queryT.collectRight

    if (readErrors.isEmpty) {
      Right(rowLocks)
    } else {
      Left(new Error(s"Querying $ctxId failed with $readErrors"))
    }
  }
}
