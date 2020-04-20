package uk.ac.wellcome.storage

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class IdentifiableTest extends AnyFunSpec with Matchers {
  it("creates an IdentityKey for Version[Id, V]") {
    val version = Version(id = "b1234", version = 5)
    version.asKey shouldBe IdentityKey("b1234/5")
  }

  it("creates an IdentityKey for classes that extend Identifiable") {
    case class ThreePartIdentifier(
      prefix: String,
      core: String,
      suffix: String
    ) extends Identifiable[String] {
      val id = s"$prefix:$core:$suffix"

      val identifier = ThreePartIdentifier("001", "apples", "5")
      identifier.asKey shouldBe IdentityKey("001:apples:5")
    }
  }
}
