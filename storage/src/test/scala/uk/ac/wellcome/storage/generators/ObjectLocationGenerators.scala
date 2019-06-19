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

  def createObjectLocationPrefixWith(
    namespace: String = randomAlphanumeric
  ): ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = namespace,
      path = randomAlphanumeric
    )

  def createObjectLocationPrefix: ObjectLocationPrefix =
    createObjectLocationPrefixWith()
}
