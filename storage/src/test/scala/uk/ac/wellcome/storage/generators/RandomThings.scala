package uk.ac.wellcome.storage.generators

import scala.util.Random

trait RandomThings {
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
}
