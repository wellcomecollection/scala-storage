package uk.ac.wellcome.storage.streaming

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

trait StreamAssertions extends Matchers {
  def assertStreamsEqual(x: InputStream, y: InputStream): Assertion =
    IOUtils.contentEquals(x, y) shouldBe true

  def assertStreamEquals(inputStream: InputStream with HasLength,
                         string: String): Assertion =
    assertStreamEquals(
      inputStream,
      string,
      expectedLength = string.getBytes.length)

  def assertStreamEquals(inputStream: InputStream with HasLength,
                         string: String,
                         expectedLength: Long): Assertion =
    assertStreamEquals(
      inputStream,
      bytes = string.getBytes(StandardCharsets.UTF_8),
      expectedLength = expectedLength
    )

  def assertStreamEquals(inputStream: InputStream with HasLength,
                         bytes: Array[Byte],
                         expectedLength: Long): Assertion = {
    inputStream.length shouldBe expectedLength

    IOUtils.contentEquals(inputStream, new ByteArrayInputStream(bytes)) shouldBe true
  }
}
