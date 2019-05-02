package uk.ac.wellcome.storage.locking

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{
  ConditionalCheckFailedException,
  PutItemResult
}
import com.gu.scanamo.{DynamoFormat, Scanamo, Table}
import com.gu.scanamo.ops.ScanamoOps

import scala.util.Try

trait ScanamoHelpers[T] {

  type ScanamoPutItemResult =
    Either[ConditionalCheckFailedException, PutItemResult]
  type ScanamoPut =
    ScanamoOps[ScanamoPutItemResult]
  type SafeEither[Out] =
    Either[Throwable, Out]

  implicit val df: DynamoFormat[T]

  protected val client: AmazonDynamoDB
  protected val table: Table[T]
  protected val index: String

  protected val delete = Scanamo.delete(client)(table.name) _
  protected val queryIndex =
    Scanamo.queryIndex[T](client)(table.name, index) _

  protected def safePutItem(ops: ScanamoPut): SafeEither[PutItemResult] =
    for {
      either <- toEither(Scanamo.exec(client)(ops))
      result <- either
    } yield result

  protected def toEither[Out](f: => Out): Either[Throwable, Out] =
    Try(f).toEither

}
