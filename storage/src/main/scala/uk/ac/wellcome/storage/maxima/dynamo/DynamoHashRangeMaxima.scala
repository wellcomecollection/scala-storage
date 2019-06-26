package uk.ac.wellcome.storage.maxima.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.syntax._
import org.scanamo.{DynamoFormat, Scanamo, Table}
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeKeyPair
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.{MaximaError, MaximaReadError, NoMaximaValueError}

import scala.util.{Failure, Success, Try}

trait DynamoHashRangeMaxima[
  HashKey, RangeKey, Row <: DynamoHashRangeKeyPair[HashKey, RangeKey]]
    extends Maxima[HashKey, RangeKey] {

  implicit protected val formatHashKey: DynamoFormat[HashKey]
  implicit protected val formatRangeKey: DynamoFormat[RangeKey]
  implicit protected val format: DynamoFormat[Row]

  protected val client: AmazonDynamoDB
  protected val table: Table[Row]

  override def max(hashKey: HashKey): Either[MaximaError, RangeKey] = {
    val ops = table.descending
      .limit(1)
      .query('id -> hashKey)

    Try(Scanamo(client).exec(ops)) match {
      case Success(List(Right(entry))) => Right(entry.rangeKey)
      case Success(List(Left(err))) =>
        val error = new Error(s"DynamoReadError: ${err.toString}")
        Left(MaximaReadError(error))
      case Success(Nil) => Left(NoMaximaValueError())
      case Failure(err) => Left(MaximaReadError(err))

      // This case should be impossible to hit in practice -- limit(1)
      // means we should only get a single result from DynamoDB.
      case result =>
        val error = new Error(
          s"Unknown error from Scanamo! $result"
        )
        Left(MaximaReadError(error))
    }
  }
}
