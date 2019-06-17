package uk.ac.wellcome.storage

import java.net.URI
import java.time.Instant

import com.gu.scanamo.DynamoFormat

package object dynamo {
  implicit val instantLongFormat: DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, String, IllegalArgumentException](
      Instant.parse
    )(
      _.toString
    )

  implicit val uriDynamoFormat: DynamoFormat[URI] =
    DynamoFormat.coercedXmap[URI, String, IllegalArgumentException](
      new URI(_)
    )(
      _.toString
    )
}
