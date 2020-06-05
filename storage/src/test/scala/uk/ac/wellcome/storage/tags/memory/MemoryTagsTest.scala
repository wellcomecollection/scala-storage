package uk.ac.wellcome.storage.tags.memory

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.tags.{Tags, TagsTestCases}

class MemoryTagsTest extends TagsTestCases[UUID, Unit] {
  override def withTags[R](initialTags: Map[UUID, Map[String, String]])(testWith: TestWith[Tags[UUID], R]): R =
    testWith(
      new MemoryTags(initialTags)
    )

  override def createIdent(context: Unit): UUID = UUID.randomUUID()

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())
}
