package uk.ac.wellcome.storage.streaming

import java.io.{FilterInputStream, InputStream}

trait HasLength {
  val length: Long
}

trait HasMetadata {
  val metadata: Map[String, String]
}

class InputStreamWithLength(inputStream: InputStream, val length: Long)
    extends FilterInputStream(inputStream) with HasLength

class InputStreamWithLengthAndMetadata(inputStream: InputStream, length: Long, val metadata: Map[String, String])
    extends InputStreamWithLength(inputStream, length) with HasMetadata

object InputStreamWithLengthAndMetadata {
  def apply(inputStream: InputStream with HasLength, metadata: Map[String, String]): InputStreamWithLengthAndMetadata =
    new InputStreamWithLengthAndMetadata(inputStream, inputStream.length, metadata)
}
