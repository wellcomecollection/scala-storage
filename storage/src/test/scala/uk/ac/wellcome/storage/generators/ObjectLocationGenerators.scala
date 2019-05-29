package uk.ac.wellcome.storage.generators

import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Random

trait ObjectLocationGenerators {
  def randomAlphanumeric: String =
    Random.alphanumeric take 8 mkString

  def createObjectLocationWith(
    namespace: String = randomAlphanumeric,
    key: String = randomAlphanumeric,
  ): ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      key = key
    )

  def createObjectLocation: ObjectLocation =
    createObjectLocationWith()
}
