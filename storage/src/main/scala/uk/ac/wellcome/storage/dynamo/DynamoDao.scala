package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.gu.scanamo.{DynamoFormat, Scanamo, Table}
import com.gu.scanamo.error.{ConditionNotMet, ScanamoError}
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.{KeyEquals, UniqueKey}
import com.gu.scanamo.syntax._
import com.gu.scanamo.update.UpdateExpression
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.Dao
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.Try

class DynamoDao[T](
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
)(
  implicit
  evidence: DynamoFormat[T],
  idGetter: IdGetter[T],
  updateExpressionGenerator: UpdateExpressionGenerator[T]
) extends Dao[String, T]
    with Logging {

  val table: Table[T] = Table[T](dynamoConfig.table)

  def put(t: T): Try[T] =
    executeOps(
      id = idGetter.id(t),
      ops = Table[T](dynamoConfig.table)
        .update(
          UniqueKey(KeyEquals('id, idGetter.id(t))),
          buildUpdate(t)
            .getOrElse(
              throw new Throwable("Could not build update expression!"))
        )
    )

  def get(id: String): Try[Option[T]] = Try {
    Scanamo.exec(dynamoClient)(table.get('id -> id)) match {
      case Some(Right(record)) =>
        debug(s"Successfully retrieved Dynamo record: $id")

        Some(record)
      case Some(Left(scanamoError)) =>
        val exception = new RuntimeException(scanamoError.toString)

        error(
          s"An error occurred while retrieving $id from DynamoDB",
          exception
        )

        throw exception
      case None =>
        debug(s"No Dynamo record found for id: $id")
        None
    }
  }

  def executeOps[S <: ScanamoError](id: String,
                                    ops: ScanamoOps[Either[S, T]]): Try[T] =
    Try {
      Scanamo.exec(dynamoClient)(ops) match {
        case Left(ConditionNotMet(exc: ConditionalCheckFailedException)) =>
          warn(s"Failed a conditional check updating $id", exc)
          throw exc
        case Left(scanamoError) =>
          val exception = new RuntimeException(scanamoError.toString)

          warn(s"Failed to update Dynamo record: $id", exception)

          throw exception
        case Right(result) =>
          debug(s"Successfully updated Dynamo record: $id")
          result
      }
    }

  def buildUpdate(t: T): Option[UpdateExpression] =
    updateExpressionGenerator
      .generateUpdateExpression(t)
}
