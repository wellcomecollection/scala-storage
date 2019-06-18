package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings

trait NamespaceFixtures[Ident, Namespace] {
  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def createId(implicit namespace: Namespace): Ident
}

trait StringNamespaceFixtures extends NamespaceFixtures[String, String] with RandomThings {
  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): String =
    s"$namespace/$randomAlphanumeric"
}
