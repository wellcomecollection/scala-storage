package uk.ac.wellcome.storage.store.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.storage.generators.MetadataGenerators
import uk.ac.wellcome.storage.streaming.Codec.bytesCodec
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait ReplayableStreamFixtures extends EitherValues with MetadataGenerators {
  // In the StoreTestCases, we need to assert that PUT and then GET returns an equivalent
  // value.  A regular InputStream gets consumed on the initial PUT, so we wrap it in
  // a ReplayableStream so we can do comparisons later.

  class ReplayableStream(val originalBytes: Array[Byte],
                         length: Long,
                         metadata: Map[String, String])
      extends InputStreamWithLengthAndMetadata(
        inputStream = bytesCodec.toStream(originalBytes).right.value,
        length = length,
        metadata = metadata
      )

  object ReplayableStream {
    def apply(bytes: Array[Byte],
              metadata: Map[String, String]): ReplayableStream =
      new ReplayableStream(bytes, length = bytes.length, metadata = metadata)
  }

  def createReplayableStream: ReplayableStream =
    ReplayableStream(randomBytes(), metadata = createValidMetadata)

}
