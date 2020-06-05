package uk.ac.wellcome.storage.store.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

trait StreamStoreFixtures[
  Ident,
  StreamStoreImpl <: StreamStore[Ident],
  StreamStoreContext]
    extends EitherValues {
  def withStreamStoreImpl[R](
    context: StreamStoreContext,
    initialEntries: Map[Ident, InputStreamWithLength])(
    testWith: TestWith[StreamStoreImpl, R]): R

  def withStreamStoreContext[R](testWith: TestWith[StreamStoreContext, R]): R
}
