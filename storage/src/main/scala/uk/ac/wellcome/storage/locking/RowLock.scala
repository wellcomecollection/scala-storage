package uk.ac.wellcome.storage.locking

import java.time.Instant
import java.time.temporal.TemporalAmount

case class RowLock(id: String,
                   contextId: String,
                   created: Instant,
                   expires: Instant)

object RowLock {
  def create(id: String, ctxId: String, duration: TemporalAmount): RowLock = {
    val created = Instant.now()

    RowLock(id, ctxId, created, created.plus(duration))
  }
}
