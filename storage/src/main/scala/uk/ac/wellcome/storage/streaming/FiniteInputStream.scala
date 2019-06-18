package uk.ac.wellcome.storage.streaming

import java.io.{FilterInputStream, InputStream}

trait FiniteStream {
  val length: Long
}

trait HasMetadata {
  val metadata: Map[String, String]
}

class FiniteInputStream(inputStream: InputStream, val length: Long)
    extends FilterInputStream(inputStream) with FiniteStream

class FiniteInputStreamWithMetadata(inputStream: InputStream, length: Long, val metadata: Map[String, String])
    extends FiniteInputStream(inputStream, length) with HasMetadata

object FiniteInputStreamWithMetadata {
  def apply(inputStream: InputStream with FiniteStream, metadata: Map[String, String]): FiniteInputStreamWithMetadata =
    new FiniteInputStreamWithMetadata(inputStream, inputStream.length, metadata)
}
