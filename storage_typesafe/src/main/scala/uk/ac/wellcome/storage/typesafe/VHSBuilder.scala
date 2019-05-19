package uk.ac.wellcome.storage.typesafe

import com.gu.scanamo.DynamoFormat
import com.typesafe.config.Config
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.dynamo.UpdateExpressionGenerator
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object VHSBuilder {
  def buildVHS[Ident, T, Metadata](config: Config, namespace: String = "vhs")(
    implicit
    evidence: DynamoFormat[BetterVHSEntry[Ident, Metadata]],
    serialisationStrategy: SerialisationStrategy[T],
    updateExpressionGenerator: UpdateExpressionGenerator[BetterVHSEntry[Ident, Metadata]]
  )
    : BetterVHS[Ident, T, Metadata] =
    new BetterVHS[Ident, T, Metadata] {
      override protected val versionedDao: VersionedDao[Ident, BetterVHSEntry[Ident, Metadata]] =
        DynamoBuilder.buildVersionedDao[Ident, BetterVHSEntry[Ident, Metadata]](config, namespace = namespace)
      override protected val objectStore: ObjectStore[T] =
        S3Builder.buildObjectStore[T](config)
      override protected val namespace: String = config
        .getOrElse[String]("aws.vhs.s3.globalPrefix")(default = "")
    }
}
