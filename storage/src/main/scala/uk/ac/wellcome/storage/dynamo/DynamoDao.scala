package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.gu.scanamo.error.ConditionNotMet
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.UniqueKey
import com.gu.scanamo.syntax._
import com.gu.scanamo.update.UpdateExpression
import com.gu.scanamo.{DynamoFormat, Scanamo, Table}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.{Failure, Success, Try}

class DynamoDao[Ident, T](
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
)(
  implicit
  evidence: DynamoFormat[T],
  idGetter: IdGetter[T],
  updateExpressionGenerator: UpdateExpressionGenerator[T]
) extends Dao[Ident, T]
    with Logging {

  val table: Table[T] = Table[T](dynamoConfig.table)

  protected def buildPutKeyExpression(t: T): UniqueKey[_] =
    'id -> idGetter.id(t)

  protected def buildGetKeyExpression(ident: Ident): UniqueKey[_] =
    'id -> ident.toString

  def put(t: T): DaoPutResult =
    executeWriteOps(
      id = idGetter.id(t),
      ops = Table[T](dynamoConfig.table)
        .update(
          buildPutKeyExpression(t),
          buildUpdate(t)
            .getOrElse(
              throw new Throwable("Could not build update expression!"))
        )
    ).map { _ => () }

  def get(id: Ident): DaoGetResult =
    executeReadOps(
      id = id,
      ops = table.get(buildGetKeyExpression(id))
    )

  def executeReadOps[A](id: Ident, ops: ScanamoOps[Option[Either[A, T]]]): DaoGetResult =
    Try {
      Scanamo.exec(dynamoClient)(ops)
    } match {
      case Failure(exc) =>
        Left(DaoReadError(exc))

      case Success(Some(Left(scanamoError))) =>
        val exc = new Throwable(scanamoError.toString)

        warn(s"Failed to read Dynamo record: $id", exc)

        Left(DaoReadError(exc))

      case Success(Some(Right(t))) =>
        debug(s"Successfully retrieved Dynamo record: $id")
        Right(t)

      case Success(None) =>
        debug(s"No Dynamo record found for id: $id")
        Left(DoesNotExistError(
          new Throwable(s"No Dynamo record found for id: $id")
        ))
    }

  def executeWriteOps[A, B](id: String, ops: ScanamoOps[Either[A, B]]): Either[WriteError with DaoError, B] =
    Try {
      Scanamo.exec(dynamoClient)(ops)
    } match {
      case Failure(exc) =>
        Left(DaoWriteError(exc))

      case Success(Left(ConditionNotMet(exc: ConditionalCheckFailedException))) =>
        warn(s"Failed a conditional check updating $id", exc)
        Left(ConditionalWriteError(exc))
      case Success(Left(scanamoError)) =>
        val exc = new Throwable(scanamoError.toString)

        warn(s"Failed to update Dynamo record: $id", exc)

        Left(DaoWriteError(exc))
      case Success(Right(result)) =>
        debug(s"Successfully updated Dynamo record: $id")
        Right(result)
    }

  def buildUpdate(t: T): Option[UpdateExpression] =
    updateExpressionGenerator
      .generateUpdateExpression(t)
}
