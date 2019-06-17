package uk.ac.wellcome.storage.maxima.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{IdentityKey, Version}
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.maxima.MaximaTestCases

class MemoryMaximaTest extends MaximaTestCases {
  override def withMaxima[R](initialEntries: Map[Version[IdentityKey, Int], Record])(testWith: TestWith[MaximaStub, R]): R = {
    val maxima = new MemoryMaxima[IdentityKey, Record] {
      override var entries: Map[Version[IdentityKey, Int], Record] = initialEntries
    }

    testWith(maxima)
  }
}

