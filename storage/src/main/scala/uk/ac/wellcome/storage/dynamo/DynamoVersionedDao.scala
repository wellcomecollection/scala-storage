package uk.ac.wellcome.storage.dynamo

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

class DynamoVersionedDao[Ident, T](
  val underlying: DynamoConditionalUpdateDao[Ident, T]
)(implicit
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T])
    extends Logging
    with VersionedDao[Ident, T]
