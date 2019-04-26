package uk.ac.wellcome.storage.locking

import java.time.Instant
import java.time.temporal.TemporalAmount

import cats.data.EitherT
import cats.implicits._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.Condition
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo, Table}
import grizzled.slf4j.Logging

import scala.concurrent.ExecutionContext
import scala.util.Try

object DynamoLockingFormats {
  implicit val instantLongFormat: AnyRef with DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](
      Instant.ofEpochSecond
    )(
      _.getEpochSecond
    )
}

trait ScanamoHelpers[T] {
  type ScanamoEither[Bad, Good] = ScanamoOps[Either[Bad, Good]]
  type SafeEither[Good] = Either[Throwable, Good]

  protected val client: AmazonDynamoDB
  protected val table: Table[T]
  protected val index: String

  protected val deleteId = Scanamo.delete(client)(table.name)
  protected val queryIndex = Scanamo.queryIndex[RowLock](client)(table.name, index)

  protected def safeExec[Bad, Good](ops: ScanamoEither[Bad, Good]): SafeEither[Good] =
    for {
      either <- toEither(Scanamo.exec(client)(ops))
      result <- either
    } yield result

  protected def toEither[Out](f: => Out) =
    Try(f).toEither
}

class DynamoLockDao(
                        dynamoClient: AmazonDynamoDB,
                        config: DynamoRowLockDaoConfig
                   )(implicit ec: ExecutionContext)
  extends LockDao[String, String]
    with Logging
    with ScanamoHelpers[RowLock] {

  import DynamoLockingFormats._

  override val client = dynamoClient
  override val table = Table[RowLock](config.dynamoConfig.table)
  override val index = config.dynamoConfig.index

  private def putLock(lock: RowLock) = {
    val hasExpired = (lock: RowLock) =>
      Condition('expires < lock.created.getEpochSecond)

    val notExists = not(attributeExists(symbol = 'id))

    table.given(hasExpired(lock) or notExists).put(lock)
  }

  override def lock(id: String, ctxId: String): Lock = {
    val rowLock = RowLock(id, ctxId, config.duration)

    trace(s"Locking $rowLock")

    safeExec(putLock(rowLock)).fold(
      e => {
        debug(s"Failed to lock $rowLock $e")
        Left(LockFailure(id, e))
      },
      result => {
        trace(s"Got $result for $rowLock")
        Right(rowLock)
      }
    )
  }

  override def unlock(ctxId: String): Unlock = {
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

  private val deleteId = Scanamo.delete(client)(table.name)
  private val queryIndex = Scanamo.queryIndex[RowLock](client)(table.name, index)

  private def getLocksFor(ctxId: String) = for {
    result <- queryRowLocks(ctxId).toEither
    rowLocks <- result
  } yield rowLocks

  private def delete(rowLock: RowLock) =
    toEither(deleteId('id -> rowLock.id))

  private def deleteRowLocks(rowLocks: List[RowLock]) = {
    val deleteT = EitherT(rowLocks.map(delete))

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

case class RowLock(id: String,
                   contextId: String,
                   created: Instant,
                   expires: Instant)

object RowLock {
  def apply(id: String, ctxId: String, duration: TemporalAmount): RowLock = {
    val created = Instant.now()

    RowLock(id, ctxId, created, created.plus(duration))
  }
}