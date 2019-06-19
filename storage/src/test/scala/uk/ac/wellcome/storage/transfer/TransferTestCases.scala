package uk.ac.wellcome.storage.transfer

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

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

    it("allows a no-op copy if the source and destination both exist and are the same") {
      withTransferStoreContext { implicit context =>
        val src = createSrcLocation
        val dst = createDstLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t, dst -> t)) { store =>
          withTransfer { transfer =>
            transfer.transfer(src, dst).right.value shouldBe TransferNoOp(src, dst)

            store.get(src) shouldBe Right(Identified(src, t))
            store.get(dst) shouldBe Right(Identified(dst, t))
          }
        }
      }
    }

    it("allows a no-op copy if the source and destination are the same") {
      withTransferStoreContext { implicit context =>
        val src = createSrcLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t)) { store =>
          withTransfer { transfer =>
            transfer.transfer(src, src).right.value shouldBe TransferNoOp(src, src)
          }
        }
      }
    }
  }
}


