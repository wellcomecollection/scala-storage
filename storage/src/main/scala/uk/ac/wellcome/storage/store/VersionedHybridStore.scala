package uk.ac.wellcome.storage.store


class VersionedHybridStore[Id, V, TypedStoreId, T, Metadata](
                                                              hybridStore: HybridStoreWithMaxima[Id, V, TypedStoreId, T, Metadata]
                                                            )(implicit N: Numeric[V])
  extends VersionedStore[Id, V, HybridStoreEntry[T, Metadata]](hybridStore)
