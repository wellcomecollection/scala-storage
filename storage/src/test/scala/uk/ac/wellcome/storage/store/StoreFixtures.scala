package uk.ac.wellcome.storage.store

import org.scalatest.{Assertion, Matchers}
import uk.ac.wellcome.fixtures.TestWith

trait StoreFixtures[Id, T, Namespace, StoreContext] extends Matchers {
  type StoreImpl = Store[Id, T]

  def withStoreImpl[R](storeContext: StoreContext, initialEntries: Map[Id, T])(testWith: TestWith[StoreImpl, R]): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R
  def withStoreContext[R](testWith: TestWith[StoreContext, R]): R

  def withStoreImpl[R](initialEntries: Map[Id, T])(testWith: TestWith[StoreImpl, R]): R =
    withStoreContext { storeContext =>
      withStoreImpl(storeContext, initialEntries) { storeImpl =>
        testWith(storeImpl)
      }
    }

  def withEmptyStoreImpl[R](testWith: TestWith[StoreImpl, R]): R =
    withStoreImpl(initialEntries = Map.empty) { storeImpl =>
      testWith(storeImpl)
    }

  def createId(implicit namespace: Namespace): Id
  def createT: T

  def assertEqualT(original: T, stored: T): Assertion =
    original shouldBe stored
}
