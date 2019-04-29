package uk.ac.wellcome.storage.locking

import java.time.Instant

import com.gu.scanamo.DynamoFormat

object DynamoLockingFormats {
  implicit val instantLongFormat: AnyRef with DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](
      Instant.ofEpochSecond
    )(
      _.getEpochSecond
    )
}
