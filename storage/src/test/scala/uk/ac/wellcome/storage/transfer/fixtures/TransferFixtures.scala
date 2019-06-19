package uk.ac.wellcome.storage.transfer.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.transfer.Transfer

trait TransferFixtures[Ident, T, StoreImpl <: Store[Ident, T], StoreContext] {
  def createSrcLocation(implicit context: StoreContext): Ident
  def createDstLocation(implicit context: StoreContext): Ident
  def createT: T

  def withTransferStoreContext[R](testWith: TestWith[StoreContext, R]): R

  def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R])(implicit context: StoreContext): R

  def withTransfer[R](testWith: TestWith[Transfer[Ident], R])(implicit context: StoreContext): R
}
