package uk.ac.wellcome.storage.typesafe

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.config.models.AWSClientConfig
import uk.ac.wellcome.storage.dynamo.{
  DynamoClientFactory,
  DynamoConfig,
  DynamoVersionedDao
}
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

  def buildVersionedDao(config: Config, namespace: String = "")(
    implicit ec: ExecutionContext): DynamoVersionedDao =
    new DynamoVersionedDao(
      dynamoDbClient = buildDynamoClient(config),
      dynamoConfig = buildDynamoConfig(config, namespace = namespace)
    )
}
