package uk.ac.wellcome.storage.generators

import java.io.{ByteArrayInputStream, InputStream}

import org.scalatest.Matchers

import scala.util.Random

trait RandomThings extends Matchers {
  def randomAlphanumeric: String =
    Random.alphanumeric take 8 mkString

  private val lowercaseLatinAlphabet = ('a' to 'z')

  def randomLowercaseLatinAlphabetChar = lowercaseLatinAlphabet(
    Random.nextInt(lowercaseLatinAlphabet.length - 1)
  )

  def randomLowercaseLatinAlphabetString(n: Int = 8) =
    (1 to n) map(_ => randomLowercaseLatinAlphabetChar) mkString

  def randomUTF16String = Random.nextString(8)

  def randomInt(from: Int, to: Int)  = {
    val difference = to - from

    assert(difference > 0)

    val randomOffset = Random.nextInt(difference) + 1

    from + randomOffset
  }

  def randomStringOfByteLength(length: Int)(utfStart: Int = 97, utfEnd: Int = 122) = {
    // Generate bytes within UTF-16 mappable range
    // 0 to 127 maps directly to Unicode code points in the ASCII range
    val chars = (1 to length).map { _ =>
      randomInt(from = utfStart, to = utfEnd).toByte.toChar
    }

    chars.mkString
  }

  def randomBytes(length: Int = 20): Array[Byte] = {
    val byteArray = Array.fill(length)(0.toByte)

    Random.nextBytes(byteArray)

    byteArray.length > 0 shouldBe true
    byteArray.length shouldBe length

    byteArray
  }

  def randomInputStream(length: Int = 256): InputStream = {
    val bytes = randomBytes(length)

    new ByteArrayInputStream(bytes)
  }
}
