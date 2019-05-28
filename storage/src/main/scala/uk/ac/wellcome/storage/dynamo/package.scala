package uk.ac.wellcome.storage

import java.net.URI
import java.time.Instant

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.DynamoReadError
import shapeless.{HList, Lazy}

package object dynamo {
  implicit val instantLongFormat: AnyRef with DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, String, IllegalArgumentException](
      Instant.parse
    )(
      _.toString
    )

  // DynamoFormat for tagged HLists
  implicit def hlistDynamoFormat[T <: HList](
    implicit formatR: Lazy[DynamoFormat.ValidConstructedDynamoFormat[T]])
    : DynamoFormat[T] =
    new DynamoFormat[T] {
      def read(av: AttributeValue): Either[DynamoReadError, T] =
        formatR.value.read(av).toEither
      def write(t: T): AttributeValue = formatR.value.write(t)
    }

  implicit val uriDynamoFormat: DynamoFormat[URI] =
    DynamoFormat.coercedXmap[URI, String, IllegalArgumentException](
      new URI(_)
    )(
      _.toString
    )
}
