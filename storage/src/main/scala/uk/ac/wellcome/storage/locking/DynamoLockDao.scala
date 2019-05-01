package uk.ac.wellcome.storage.locking

import java.util.UUID

import cats.data.EitherT
import cats.implicits._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{
  DeleteItemResult,
  PutItemResult
}
import com.gu.scanamo.DynamoFormat._
import com.gu.scanamo.query.Condition
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Table}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{LockDao, LockFailure, UnlockFailure}

import scala.concurrent.ExecutionContext
import scala.util.Try

class DynamoLockDao(
  val client: AmazonDynamoDB,
  config: DynamoLockDaoConfig
)(implicit
  val ec: ExecutionContext,
  val df: DynamoFormat[ExpiringLock])
    extends LockDao[String, UUID]
    with Logging
    with ScanamoHelpers[ExpiringLock] {

  override val table: Table[ExpiringLock] = Table[ExpiringLock](config.dynamoConfig.table)
  override val index: String = config.dynamoConfig.index

  // Lock

  override def lock(id: Ident, contextId: ContextId): LockResult = {
    val rowLock = ExpiringLock.create(
      id = id,
      contextId = contextId,
      duration = config.duration
    )

    debug(s"Locking $rowLock: START")

    lockOp(rowLock).fold(
      e => {
        debug(s"Locking $rowLock: FAILED ($e)")
        Left(LockFailure(id, e))
      },
      result => {
        debug(s"Locking $rowLock: SUCCESS ($result)")
        Right(rowLock)
      }
    )
  }

  private def lockOp(lock: ExpiringLock): Either[Throwable, PutItemResult] = {
    val lockFound = attributeExists(symbol = 'id)
    val lockNotFound = not(lockFound)

    val isExpired = Condition('expires < lock.created.toString)

    val lockHasExpired = Condition(lockFound and isExpired)

    val lockAlreadyExists = Condition('contextId -> lock.contextId)

    val canLock =
      lockHasExpired or lockNotFound or lockAlreadyExists

    safePutItem(table.given(canLock).put(lock))
  }

  // Unlock

  override def unlock(contextId: ContextId): UnlockResult = {
    debug(s"Unlocking $contextId: START")

    queryAndDelete(contextId).fold(
      e => {
        warn(s"Unlocking $contextId: FAILED ($e)")
        Left(UnlockFailure(contextId, e))
      },
      result => {
        trace(s"Unlocking $contextId: SUCCESS ($result)")
        Right(result)
      }
    )
  }

  private def queryAndDelete(contextId: ContextId): Either[Throwable, Unit] =
    for {
      queryOp <- queryLocks(contextId).toEither
      rowLocks <- queryOp
      _ <- deleteLocks(rowLocks)
    } yield ()

  private def deleteLocks(rowLocks: List[ExpiringLock]): Either[Error, List[DeleteItemResult]] = {
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

  private def queryLocks(contextId: ContextId) = Try {
    val queryT = EitherT(queryIndex('contextId -> contextId))

    val readErrors = queryT.swap.collectRight
    val rowLocks = queryT.collectRight

    if (readErrors.isEmpty) {
      Right(rowLocks)
    } else {
      Left(new Error(s"Querying $contextId failed with $readErrors"))
    }
  }
}
