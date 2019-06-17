package uk.ac.wellcome.storage

import java.net.{URI, URISyntaxException}

import org.scanamo.DynamoFormat

package object dynamo {
  implicit val uriDynamoFormat: DynamoFormat[URI] =
    DynamoFormat.coercedXmap[URI, String, URISyntaxException](
      new URI(_)
    )(
      _.toString
    )
}
