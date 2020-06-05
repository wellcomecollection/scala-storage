package uk.ac.wellcome.storage.store.memory

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class MemoryStreamStore[Ident](val memoryStore: MemoryStore[Ident, Array[Byte]])
    extends StreamStore[Ident]
    with Logging {
  override def get(id: Ident): ReadEither =
    for {
      entry <- memoryStore.get(id)
      bytes = entry.identifiedT

      // This is sort of cheating, but since we created these streams the lengths
      // should never be incorrect, and it means we can't get an EncoderError
      // (a WriteError) from inside a REad method.
      inputStream = bytesCodec.toStream(bytes).right.get
    } yield Identified(id, inputStream)

  override def put(id: Ident)(entry: InputStreamWithLength): WriteEither =
    bytesCodec.fromStream(entry) match {
      case Right(bytes) =>
        memoryStore.put(id)(bytes).map { _ =>
          Identified(id, entry)
        }

      case Left(err: IncorrectStreamLengthError) => Left(err)

      case Left(err: DecoderError) => Left(StoreWriteError(err.e))
    }
}

object MemoryStreamStore {
  def apply[Ident](): MemoryStreamStore[Ident] = {
    val memoryStore =
      new MemoryStore[Ident, Array[Byte]](initialEntries = Map.empty)

    new MemoryStreamStore[Ident](memoryStore)
  }
}
