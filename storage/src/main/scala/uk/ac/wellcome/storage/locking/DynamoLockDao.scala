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
                        dynamoClient: AmazonDynamoDB,
                        config: DynamoRowLockDaoConfig
                   )(implicit val ec: ExecutionContext, val df: DynamoFormat[RowLock])
  extends LockDao[String, String]
    with Logging
    with ScanamoHelpers[RowLock] {

  override val client = dynamoClient
  override val table = Table[RowLock](config.dynamoConfig.table)
  override val index = config.dynamoConfig.index

  override def lock(id: String, ctxId: String): Lock = {
    val rowLock = RowLock.create(id, ctxId, config.duration)

    debug(s"Locking $rowLock")

    safePutItem(putLock(rowLock)).fold(
      e => {
        debug(s"Failed to lock $rowLock $e")
        Left(LockFailure(id, e))
      },
      result => {
        debug(s"Got $result for $rowLock")
        Right(rowLock)
      }
    )
  }

  override def unlock(ctxId: String): Unlock = {
    debug(s"Unlocking $ctxId")

    val unlockOp = for {
      queryOp <- queryRowLocks(ctxId).toEither
      rowLocks <- queryOp
      deletions <- deleteRowLocks(rowLocks)
    } yield deletions

    unlockOp.fold(
      e => {
        warn(s"Failed to unlock $ctxId $e")
        Left(UnlockFailure(ctxId, e))
      },
      _ => {
        trace(s"Unlocked $ctxId")
        Right(())
      }
    )
  }

  private def putLock(lock: RowLock) = {
    val hasExpired = (lock: RowLock) =>
      Condition('expires < lock.created.getEpochSecond)

    val notExists = not(attributeExists(symbol = 'id))

    table.given(hasExpired(lock) or notExists).put(lock)
  }

  private def deleteRowLocks(rowLocks: List[RowLock]) = {
    val deleteT = EitherT(rowLocks.map(
      rowLock => toEither(delete('id -> rowLock.id))))

    val deleteErrors = deleteT.swap.collectRight
    val deletions = deleteT.collectRight

    if(deleteErrors.isEmpty) {
      Right(deletions)
    } else {
      Left(new Error(s"Deleting $rowLocks failed with $deleteErrors"))
    }
  }

  private def queryRowLocks(ctxId: String) = Try {
    val queryT = EitherT(queryIndex('contextId -> ctxId))

    val readErrors = queryT.swap.collectRight
    val rowLocks = queryT.collectRight

    if(readErrors.isEmpty) {
      Right(rowLocks)
    } else {
      Left(new Error(s"Querying $ctxId failed with $readErrors"))
    }
  }
}