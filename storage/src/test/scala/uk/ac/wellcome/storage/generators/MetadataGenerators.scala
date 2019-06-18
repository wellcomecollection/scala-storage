package uk.ac.wellcome.storage.generators

trait MetadataGenerators extends RandomThings {
  def createValidMetadata: Map[String, String] = {
    val metadata = (1 to 10)
      .map { _ =>
        (randomLowercaseLatinAlphabetString(), randomLowercaseLatinAlphabetString())
      }

    metadata.toMap
  }
}
