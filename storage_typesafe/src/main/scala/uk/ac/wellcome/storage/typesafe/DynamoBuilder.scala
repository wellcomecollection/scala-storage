package uk.ac.wellcome.storage.typesafe

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.DynamoFormat
import com.typesafe.config.Config
import uk.ac.wellcome.config.models.AWSClientConfig
import uk.ac.wellcome.storage.dynamo.{DynamoClientFactory, DynamoConfig, DynamoVersionedDao, UpdateExpressionGenerator}
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}
import uk.ac.wellcome.typesafe.config.builders.AWSClientConfigBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object DynamoBuilder extends AWSClientConfigBuilder {
  def buildDynamoConfig(config: Config,
                        namespace: String = ""): DynamoConfig = {
    val tableName = config
      .required[String](s"aws.$namespace.dynamo.tableName")
    val tableIndex = config
      .getOrElse[String](s"aws.$namespace.dynamo.tableIndex")(default = "")

    DynamoConfig(
      table = tableName,
      maybeIndex = if (tableIndex.isEmpty) None else Some(tableIndex)
    )
  }

  private def buildDynamoClient(
    awsClientConfig: AWSClientConfig): AmazonDynamoDB =
    DynamoClientFactory.create(
      region = awsClientConfig.region,
      endpoint = awsClientConfig.endpoint.getOrElse(""),
      accessKey = awsClientConfig.accessKey.getOrElse(""),
      secretKey = awsClientConfig.secretKey.getOrElse("")
    )

  def buildDynamoClient(config: Config): AmazonDynamoDB =
    buildDynamoClient(
      awsClientConfig = buildAWSClientConfig(config, namespace = "dynamo")
    )

  def buildVersionedDao[T](config: Config, namespace: String = "")(
    implicit
    ec: ExecutionContext,
    evidence: DynamoFormat[T],
    versionUpdater: VersionUpdater[T],
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T]): DynamoVersionedDao[T] =
    new DynamoVersionedDao[T](
      dynamoDbClient = buildDynamoClient(config),
      dynamoConfig = buildDynamoConfig(config, namespace = namespace)
    )
}
