package uk.ac.wellcome.storage.generators

import uk.ac.wellcome.storage.ObjectLocation

trait ObjectLocationGenerators extends RandomThings {
  def createObjectLocationWith(
    namespace: String = randomAlphanumeric,
    key: String = randomAlphanumeric,
  ): ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = key
    )

  def createObjectLocation: ObjectLocation =
    createObjectLocationWith()
}
