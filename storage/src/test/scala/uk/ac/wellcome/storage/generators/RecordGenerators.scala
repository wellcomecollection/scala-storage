package uk.ac.wellcome.storage.generators

import java.io.ByteArrayInputStream

import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.IdentityKey
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.{Codec, Decoder, Encoder, FiniteInputStream}

case class Record(name: String)

object Record {
  def serialize(record: Record)(implicit encoder: Encoder[Record]): Array[Byte] = {
    val inputStream = encoder.toStream(record).right.get

    IOUtils.toByteArray(inputStream)
  }

  def deserialize(bytes: Array[Byte])(implicit decoder: Decoder[Record]): Record = {
    val bis = new ByteArrayInputStream(bytes)
    val fis = new FiniteInputStream(bis, bytes.length)

    decoder.fromStream(fis).right.get
  }
}

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
