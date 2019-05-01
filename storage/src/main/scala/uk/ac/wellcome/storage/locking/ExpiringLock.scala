package uk.ac.wellcome.storage.locking

import java.time.Instant
import java.time.temporal.TemporalAmount
import java.util.UUID

import uk.ac.wellcome.storage.Lock

case class ExpiringLock(id: String,
                        contextId: UUID,
                        created: Instant,
                        expires: Instant)
    extends Lock[String, UUID]

object ExpiringLock {
  def create(id: String,
             contextId: UUID,
             duration: TemporalAmount): ExpiringLock = {
    val created = Instant.now()

    ExpiringLock(
      id = id,
      contextId = contextId,
      created = created,
      expires = created.plus(duration)
    )
  }
}
