package uk.ac.wellcome.storage.transfer

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.memory.MemoryTransfer

trait TransferFixtures[Ident, T, StoreImpl <: Store[Ident, T], StoreContext] {
  def createSrcLocation(implicit context: StoreContext): Ident
  def createDstLocation(implicit context: StoreContext): Ident
  def createT: T

  def withTransferStoreContext[R](testWith: TestWith[StoreContext, R]): R

  def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R])(implicit context: StoreContext): R

  def withTransfer[R](testWith: TestWith[Transfer[Ident], R])(implicit context: StoreContext): R
}

trait TransferTestCases[Ident, T, StoreImpl <: Store[Ident, T], StoreContext] extends FunSpec with Matchers with EitherValues with TransferFixtures[Ident, T, StoreImpl, StoreContext] {
  describe("behaves as a Transfer") {
    it("copies an object from a source to a destination") {
      withTransferStoreContext { implicit context =>
        val src = createSrcLocation
        val dst = createDstLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t)) { store =>
          withTransfer { transfer =>
            transfer.transfer(src, dst).right.value shouldBe TransferPerformed(src, dst)

            store.get(src) shouldBe Right(Identified(src, t))
            store.get(dst) shouldBe Right(Identified(dst, t))
          }
        }
      }
    }

    it("errors if the source does not exist") {
      withTransferStoreContext { implicit context =>
        val src = createSrcLocation
        val dst = createDstLocation

        withTransfer { transfer =>
          val err = transfer.transfer(src, dst).left.get
          err shouldBe a[TransferSourceFailure[_]]
          err.asInstanceOf[TransferSourceFailure[Ident]].source shouldBe src
          err.asInstanceOf[TransferSourceFailure[Ident]].destination shouldBe dst
        }
      }
    }

    it("errors if the source and destination both exist and are different") {
      withTransferStoreContext { implicit context =>
        val src = createSrcLocation
        val dst = createDstLocation

        withTransfer { transfer =>
          withTransferStore(initialEntries = Map(src -> createT, dst -> createT)) { store =>
            val err = transfer.transfer(src, dst).left.get
            err shouldBe a[TransferOverwriteFailure[_]]
            err.asInstanceOf[TransferOverwriteFailure[Ident]].source shouldBe src
            err.asInstanceOf[TransferOverwriteFailure[Ident]].destination shouldBe dst
          }
        }
      }
    }
  }
}

trait MemoryTransferFixtures[Ident, T] extends TransferFixtures[Ident, T, MemoryStore[Ident, T], MemoryStore[Ident, T]] {
  override def withTransferStoreContext[R](testWith: TestWith[MemoryStore[Ident, T], R]): R =
    testWith(new MemoryStore[Ident, T](initialEntries = Map.empty))

  override def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[MemoryStore[Ident, T], R])(implicit store: MemoryStore[Ident, T]): R = {
    store.entries = store.entries ++ initialEntries
    testWith(store)
  }

  override def withTransfer[R](testWith: TestWith[Transfer[Ident], R])(implicit underlying: MemoryStore[Ident, T]): R =
    testWith(new MemoryTransfer[Ident, T](underlying))
}

class MemoryTransferTestCases extends TransferTestCases[String, Array[Byte], MemoryStore[String, Array[Byte]], MemoryStore[String, Array[Byte]]] with MemoryTransferFixtures[String, Array[Byte]] with RandomThings {
  override def createSrcLocation(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric

  override def createDstLocation(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric

  override def createT: Array[Byte] = randomBytes()
}
