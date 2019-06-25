package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._


case class HybridStoreEntry[T, Metadata](t: T, metadata: Metadata)
case class HybridIndexedStoreEntry[IndexedStoreId, TypeStoreId, Metadata](
                                          indexedStoreId: IndexedStoreId,
                                          typeStoreId: TypeStoreId,
                                          metadata: Metadata
                                        )

trait HybridStore[IndexedStoreId, TypedStoreId, T, Metadata]
  extends Store[IndexedStoreId, HybridStoreEntry[T, Metadata]]
    with Logging {

  type IndexEntry = HybridIndexedStoreEntry[IndexedStoreId, TypedStoreId, Metadata]

  implicit protected val indexedStore: Store[IndexedStoreId, IndexEntry]
  implicit protected val typedStore: TypedStore[TypedStoreId, T]

  protected def createTypeStoreId(id: IndexedStoreId): TypedStoreId

  override def get(id: IndexedStoreId): ReadEither = for {
    indexResult <- indexedStore.get(id)

    indexedStoreEntry = indexResult.identifiedT

    typeStoreEntry <- getTypedStoreEntry(indexedStoreEntry.typeStoreId)

    typeStoreId = indexedStoreEntry.typeStoreId
    metadata    = indexedStoreEntry.metadata
    hybridEntry: HybridStoreEntry[T, Metadata] = HybridStoreEntry(typeStoreEntry.identifiedT.t, metadata)

  } yield Identified(id, hybridEntry)

  // If the indexed store points to a typed store entry that doesn't exist, that
  // suggests an internal error in the store, so we don't want to bubble up
  // the DoesNotExistError directly.
  private def getTypedStoreEntry(typedStoreId: TypedStoreId): Either[ReadError, Identified[TypedStoreId, TypedStoreEntry[T]]] =
    typedStore.get(typedStoreId) match {
      case Right(t) => Right(t)
      case Left(err: DoesNotExistError) => Left(DanglingHybridStorePointerError(err.e))
      case Left(err) => Left(err)
    }

  override def put(id: IndexedStoreId)(t: HybridStoreEntry[T, Metadata]): WriteEither = {
    val typeStoreId = createTypeStoreId(id)

     for {
       putTypeResult <- typedStore.put(typeStoreId)(TypedStoreEntry(t.t, Map.empty))

       locationEntry = HybridIndexedStoreEntry(
         indexedStoreId = id,
         typeStoreId = putTypeResult.id,
         metadata = t.metadata
       )

       putVersionedResult <- indexedStore
         .put(id)(locationEntry) match {
            case Left(error) => Left(StoreWriteError(error.e))
            case Right(result) => Right(result)
          }

       hybridEntry = HybridStoreEntry(
         putTypeResult.identifiedT.t,
         putVersionedResult.identifiedT.metadata
       )

    } yield Identified(putVersionedResult.id, hybridEntry)
  }
}

