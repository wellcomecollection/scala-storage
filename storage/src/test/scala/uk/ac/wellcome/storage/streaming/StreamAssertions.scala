package uk.ac.wellcome.storage.streaming

import java.io.InputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.scalatest.{Assertion, Matchers}

trait StreamAssertions extends Matchers {
  def assertStreamsEqual(x: InputStream, y: InputStream): Assertion =
    IOUtils.contentEquals(x, y) shouldBe true

  def assertStreamEquals(inputStream: FiniteInputStream, string: String): Assertion =
    assertStreamEquals(inputStream, string, expectedLength = string.getBytes.length)

  def assertStreamEquals(inputStream: FiniteInputStream, string: String, expectedLength: Long): Assertion = {
    inputStream.length shouldBe expectedLength

    IOUtils.contentEquals(
      inputStream,
      IOUtils.toInputStream(
        string,
        StandardCharsets.UTF_8
      )
    ) shouldBe true
  }
}