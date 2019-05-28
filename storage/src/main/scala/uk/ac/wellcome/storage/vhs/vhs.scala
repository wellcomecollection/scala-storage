package uk.ac.wellcome.storage

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.DynamoReadError
import uk.ac.wellcome.storage.dynamo.UpdateExpressionGenerator

package object vhs {
  implicit val vhsEntryFormat: DynamoFormat[Entry[String, EmptyMetadata]] =
    new DynamoFormat[Entry[String, EmptyMetadata]] {
      override def read(av: AttributeValue)
        : Either[DynamoReadError, Entry[String, EmptyMetadata]] =
        DynamoFormat[PlainEntry[String]].read(av).map { plainEntry =>
          Entry(
            id = plainEntry.id,
            version = plainEntry.version,
            location = plainEntry.location,
            metadata = EmptyMetadata()
          )
        }

      override def write(
        entry: Entry[String, EmptyMetadata]): AttributeValue = {
        val plainEntry = PlainEntry[String](
          id = entry.id,
          version = entry.version,
          location = entry.location
        )

        DynamoFormat[PlainEntry[String]].write(plainEntry)
      }
    }

  implicit val updateExpressionGenerator
    : UpdateExpressionGenerator[Entry[String, EmptyMetadata]] =
    (entry: Entry[String, EmptyMetadata]) =>
      UpdateExpressionGenerator[PlainEntry[String]].generateUpdateExpression(
        PlainEntry[String](
          id = entry.id,
          version = entry.version,
          location = entry.location
        )
    )
}
