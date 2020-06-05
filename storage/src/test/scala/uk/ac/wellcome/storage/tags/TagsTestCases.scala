package uk.ac.wellcome.storage.tags

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{DoesNotExistError, UpdateNoSourceError, UpdateNotApplied}
import uk.ac.wellcome.storage.generators.RandomThings

trait TagsTestCases[Ident] extends AnyFunSpec with Matchers with EitherValues with RandomThings {
  def withTags[R](initialTags: Map[Ident, Map[String, String]])(testWith: TestWith[Tags[Ident], R]): R

  def createIdent: Ident

  def createTags: Map[String, String] =
    (1 to randomInt(from = 0, to = 25))
      .map { _ =>
        randomAlphanumeric -> randomAlphanumeric
      }
      .toMap

  describe("behaves as a Tags") {
    describe("get()") {
      it("can read the tags for an identifier") {
        val objectIdent = createIdent
        val objectTags = createTags

        withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
          tags.get(id = objectIdent).right.value shouldBe objectTags
        }
      }

      it("returns a DoesNotExistError if the ident does nto exist") {
        withTags(initialTags = Map.empty) { tags =>
          tags.get(id = createIdent).left.value shouldBe a[DoesNotExistError]
        }
      }
    }

    describe("update()") {
      it("appends to the tags on an object") {
        val objectIdent = createIdent
        val objectTags = createTags

        val newTag = Map("myTag" -> "newTag")

        withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
          tags
            .update(id = objectIdent) { existingTags =>
              Right(existingTags ++ newTag)
            }
            .right.value shouldBe objectTags ++ newTag

          tags.get(id = objectIdent).right.value shouldBe objectTags ++ newTag
        }
      }

      it("can delete tags on an object") {
        val objectIdent = createIdent
        val objectTags = createTags

        withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
          tags
            .update(id = objectIdent) { existingTags =>
              Right(Map.empty)
            }
            .right.value shouldBe Map.empty

          tags.get(id = objectIdent).right.value shouldBe Map.empty
        }
      }

      it("doesn't change the tags if the update function returns a Left()") {
        val objectIdent = createIdent
        val objectTags = createTags

        withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
          tags
            .update(id = objectIdent) { _ =>
              Left(UpdateNotApplied(new Throwable("BOOM!")))
            }
            .left.value shouldBe a[UpdateNotApplied]

          tags.get(id = objectIdent).right.value shouldBe objectTags
        }
      }

      it("can apply a no-op update on tags") {
        val objectIdent = createIdent
        val objectTags = createTags

        withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
          tags
            .update(id = objectIdent) { Right(_) }
            .right.value shouldBe objectTags

          tags.get(id = objectIdent).right.value shouldBe objectTags
        }
      }

      it("returns an UpdateSourceError if the ident does not exist") {
        val objectIdent = createIdent

        withTags(initialTags = Map.empty) { tags =>
          tags
            .update(id = objectIdent) { Right(_) }
            .left.value shouldBe a[UpdateNoSourceError]

          tags.get(id = objectIdent).left.value shouldBe a[DoesNotExistError]
        }
      }
    }
  }
}
