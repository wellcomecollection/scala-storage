package uk.ac.wellcome.storage.typesafe

import com.typesafe.config.Config
import org.scanamo.DynamoFormat
import uk.ac.wellcome.storage.locking.dynamo.{
  DynamoLockDao,
  DynamoLockDaoConfig,
  ExpiringLock
}
import uk.ac.wellcome.typesafe.config.builders.AWSClientConfigBuilder

import scala.concurrent.ExecutionContext

object DynamoLockDaoBuilder extends AWSClientConfigBuilder {
  def buildDynamoLockDao(config: Config, namespace: String = "locking")(
    implicit
    ec: ExecutionContext,
    df: DynamoFormat[ExpiringLock]
  ) = new DynamoLockDao(
    client = DynamoBuilder.buildDynamoClient(config),
    config =
      DynamoLockDaoConfig(DynamoBuilder.buildDynamoConfig(config, namespace))
  )
}
