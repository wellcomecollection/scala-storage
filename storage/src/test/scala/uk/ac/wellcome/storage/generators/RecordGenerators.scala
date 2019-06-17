package uk.ac.wellcome.storage.generators

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.IdentityKey
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.streaming.Codec._

case class Record(name: String)

trait RecordGenerators extends RandomThings with Logging {
  implicit val codec: Codec[Record] = typeCodec[Record]

  val recordCount: (Int, Int) = (100, 200)

  def createIdentityKey: IdentityKey =
    IdentityKey(randomAlphanumeric)

  def createRecord: Record = {
    val record = Record(name = randomAlphanumeric)

    trace(s"Created Record: $record")

    record
  }

  def createRecords: Set[Record] = {
    val (start, end) = recordCount
    (1 to randomInt(from = start, to = end)).map(_ => createRecord).toSet
  }
}
