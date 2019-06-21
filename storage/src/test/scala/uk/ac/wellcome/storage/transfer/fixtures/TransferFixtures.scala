package uk.ac.wellcome.storage.transfer.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.transfer.Transfer

trait TransferFixtures[Ident, T, StoreImpl <: Store[Ident, T], TransferImpl <: Transfer[Ident]] {
  def createT: T

  def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R]): R

  def withTransfer[R](testWith: TestWith[TransferImpl, R])(implicit store: StoreImpl): R
}
