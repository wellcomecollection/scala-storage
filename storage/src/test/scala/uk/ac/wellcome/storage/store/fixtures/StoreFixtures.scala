package uk.ac.wellcome.storage.store.fixtures

import org.scalatest.{Assertion, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.Store

trait StoreFixtures[Ident, T, Namespace, StoreContext] extends Matchers with NamespaceFixtures[Ident, Namespace] {
  type StoreImpl = Store[Ident, T]

  def withStoreImpl[R](storeContext: StoreContext, initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R]): R

  def withStoreContext[R](testWith: TestWith[StoreContext, R]): R

  def withStoreImpl[R](initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R]): R =
    withStoreContext { storeContext =>
      withStoreImpl(storeContext, initialEntries) { storeImpl =>
        testWith(storeImpl)
      }
    }

  def withEmptyStoreImpl[R](testWith: TestWith[StoreImpl, R]): R =
    withStoreImpl(initialEntries = Map.empty) { storeImpl =>
      testWith(storeImpl)
    }

  def createT: T

  def assertEqualT(original: T, stored: T): Assertion =
    original shouldBe stored
}
