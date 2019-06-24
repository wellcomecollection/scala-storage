package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.{Store, VersionedStore, Writable, Readable}
import uk.ac.wellcome.storage._

trait VersionedStoreFixtures[Id, T] {
  type StoreImpl = Store[Version[Id, Int], T] with Maxima[Id, Int]
  type VersionedStoreImpl = VersionedStore[Id, Int, T]

  trait FailingGet extends Readable[Version[Id, Int], T] {
    override def get(v: Version[Id, Int]): Either[ReadError, Identified[Version[Id, Int], T]] =
      Left(StoreReadError(new RuntimeException("boom")))
  }

  trait FailingPut extends Writable[Version[Id, Int], T] {
    override def put(v: Version[Id, Int])(t: T): Either[WriteError, Identified[Version[Id, Int], T]] =
      Left(StoreWriteError(new RuntimeException("boom")))
  }
}