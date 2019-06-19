package uk.ac.wellcome.storage.generators

import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait ObjectLocationGenerators extends RandomThings {
  def createObjectLocationWith(
    namespace: String = randomAlphanumeric,
    path: String = randomAlphanumeric,
  ): ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = path
    )

  def createObjectLocation: ObjectLocation =
    createObjectLocationWith()

  def createObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = randomAlphanumeric,
      path = randomAlphanumeric
    )
}
