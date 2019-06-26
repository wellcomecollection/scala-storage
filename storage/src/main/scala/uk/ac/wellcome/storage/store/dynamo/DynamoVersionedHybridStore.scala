package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}

class DynamoVersionedHybridStore[T, Metadata](
  store: DynamoHybridStoreWithMaxima[T, Metadata])
    extends VersionedStore[String, Int, HybridStoreEntry[T, Metadata]](store)
