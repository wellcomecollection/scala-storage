package uk.ac.wellcome.storage.tags

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.DoesNotExistError
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
  }
}
