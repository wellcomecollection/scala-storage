package uk.ac.wellcome.storage.generators

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

  def randomBytes(length: Int = 20): Array[Byte] = {
    val byteArray = Array[Byte](20)
    Random.nextBytes(byteArray)

    byteArray.length > 0 shouldBe true

    byteArray
  }
}
