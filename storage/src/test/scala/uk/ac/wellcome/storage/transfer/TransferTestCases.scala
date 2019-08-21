package uk.ac.wellcome.storage.transfer

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait TransferTestCases[
  Location, T, Namespace, StoreImpl <: Store[Location, T]]
    extends FunSpec
    with Matchers
    with EitherValues
    with TransferFixtures[Location, T, StoreImpl]
    with NamespaceFixtures[Location, Namespace] {
  def createSrcLocation(implicit namespace: Namespace): Location
  def createDstLocation(implicit namespace: Namespace): Location

  describe("behaves as a Transfer") {
    it("copies an object from a source to a destination") {
      withNamespace { implicit namespace =>
        val src = createSrcLocation
        val dst = createDstLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t)) { implicit store =>
          withTransfer { transfer =>
            transfer.transfer(src, dst).right.value shouldBe TransferPerformed(
              src,
              dst)

            store.get(src) shouldBe Right(Identified(src, t))
            store.get(dst) shouldBe Right(Identified(dst, t))
          }
        }
      }
    }

    it("errors if the source does not exist") {
      withNamespace { implicit namespace =>
        val src = createSrcLocation
        val dst = createDstLocation

        withTransferStore(initialEntries = Map.empty) { implicit store =>
          withTransfer { transfer =>
            val err = transfer.transfer(src, dst).left.get
            err shouldBe a[TransferSourceFailure[_]]
            err
              .asInstanceOf[TransferSourceFailure[Location]]
              .source shouldBe src
            err
              .asInstanceOf[TransferSourceFailure[Location]]
              .destination shouldBe dst
          }
        }
      }
    }

    it("errors if the source and destination both exist and are different") {
      withNamespace { implicit namespace =>
        val src = createSrcLocation
        val dst = createDstLocation

        withTransferStore(initialEntries = Map(src -> createT, dst -> createT)) {
          implicit store =>
            withTransfer { transfer =>
              val err = transfer.transfer(src, dst).left.get
              err shouldBe a[TransferOverwriteFailure[_]]
              err
                .asInstanceOf[TransferOverwriteFailure[Location]]
                .source shouldBe src
              err
                .asInstanceOf[TransferOverwriteFailure[Location]]
                .destination shouldBe dst
            }
        }
      }
    }

    it(
      "allows a no-op copy if the source and destination both exist and are the same") {
      withNamespace { implicit namespace =>
        val src = createSrcLocation
        val dst = createDstLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t, dst -> t)) {
          implicit store =>
            withTransfer { transfer =>
              transfer.transfer(src, dst).right.value shouldBe TransferNoOp(
                src,
                dst)

              store.get(src) shouldBe Right(Identified(src, t))
              store.get(dst) shouldBe Right(Identified(dst, t))
            }
        }
      }
    }

    it("allows a no-op copy if the source and destination are the same") {
      withNamespace { implicit namespace =>
        val src = createSrcLocation
        val t = createT

        withTransferStore(initialEntries = Map(src -> t)) { implicit store =>
          withTransfer { transfer =>
            transfer.transfer(src, src).right.value shouldBe TransferNoOp(
              src,
              src)
          }
        }
      }
    }
  }
}
