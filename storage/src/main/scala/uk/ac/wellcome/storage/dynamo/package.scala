package uk.ac.wellcome.storage

import java.net.URI

import org.scanamo.DynamoFormat

package object dynamo {
  implicit val uriDynamoFormat: DynamoFormat[URI] =
    DynamoFormat.coercedXmap[URI, String, IllegalArgumentException](
      new URI(_)
    )(
      _.toString
    )
}
