package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.store.VersionedHybridStore

class DynamoVersionedHybridStore[Id, V, T, Metadata](
  store: DynamoHybridStoreWithMaxima[Id, V, T, Metadata])(
  implicit N: Numeric[V])
    extends VersionedHybridStore[Id, V, S3ObjectLocation, T, Metadata](store)
