package uk.ac.wellcome.storage.typesafe

import com.gu.scanamo.DynamoFormat
import com.typesafe.config.Config
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo.UpdateExpressionGenerator
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}
import uk.ac.wellcome.storage.vhs.{Entry, VersionedHybridStore}
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object VHSBuilder {
  def buildVHS[Ident, T, Metadata](config: Config, configNamespace: String = "vhs")(
    implicit
    evidence: DynamoFormat[Entry[Ident, Metadata]],
    idGetter: IdGetter[Entry[Ident, Metadata]],
    serialisationStrategy: SerialisationStrategy[T],
    versionGetter: VersionGetter[Entry[Ident, Metadata]],
    versionUpdater: VersionUpdater[Entry[Ident, Metadata]],
    updateExpressionGenerator: UpdateExpressionGenerator[Entry[Ident, Metadata]]
  )
    : VersionedHybridStore[Ident, T, Metadata] =
    new VersionedHybridStore[Ident, T, Metadata] {
      override protected val versionedDao: VersionedDao[Ident, Entry[Ident, Metadata]] =
        DynamoBuilder.buildVersionedDao[Ident, Entry[Ident, Metadata]](config, namespace = configNamespace)
      override protected val objectStore: ObjectStore[T] =
        S3Builder.buildObjectStore[T](config)
      override protected val namespace: String = config
        .getOrElse[String]("aws.vhs.s3.globalPrefix")(default = "")
    }
}
