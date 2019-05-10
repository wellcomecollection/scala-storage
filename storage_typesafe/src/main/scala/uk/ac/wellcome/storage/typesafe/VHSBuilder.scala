package uk.ac.wellcome.storage.typesafe

import com.typesafe.config.Config
import uk.ac.wellcome.storage.BetterObjectStore
import uk.ac.wellcome.storage.type_classes.SerialisationStrategy
import uk.ac.wellcome.storage.vhs.{VHSConfig, VersionedHybridStore}
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object VHSBuilder {
  def buildVHSConfig(config: Config): VHSConfig = {
    val s3Config = S3Builder.buildS3Config(config, namespace = "vhs")
    val dynamoConfig =
      DynamoBuilder.buildDynamoConfig(config, namespace = "vhs")

    val globalS3Prefix = config
      .getOrElse[String]("aws.vhs.s3.globalPrefix")(default = "")

    VHSConfig(
      dynamoConfig = dynamoConfig,
      s3Config = s3Config,
      globalS3Prefix = globalS3Prefix
    )
  }

  def buildVHS[T, M](config: Config)(
    implicit serialisationStrategy: SerialisationStrategy[T])
    : VersionedHybridStore[T, M, BetterObjectStore[T]] = {
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    new VersionedHybridStore[T, M, BetterObjectStore[T]](
      vhsConfig = buildVHSConfig(config),
      objectStore = S3Builder.buildObjectStore[T](config),
      dynamoDbClient = DynamoBuilder.buildDynamoClient(config)
    )
  }
}
