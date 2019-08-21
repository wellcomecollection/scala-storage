package uk.ac.wellcome.storage.store.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait StreamStoreFixtures[
  Ident,
  StreamStoreImpl <: StreamStore[Ident, InputStreamWithLengthAndMetadata],
  StreamStoreContext]
    extends EitherValues {
  def withStreamStoreImpl[R](
    context: StreamStoreContext,
    initialEntries: Map[Ident, InputStreamWithLengthAndMetadata])(
    testWith: TestWith[StreamStoreImpl, R]): R

  def withStreamStoreContext[R](testWith: TestWith[StreamStoreContext, R]): R
}
