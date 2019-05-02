package uk.ac.wellcome.storage.typesafe

import java.time.Duration

import com.typesafe.config.Config
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.locking.{DynamoLockDao, DynamoLockDaoConfig, DynamoLockingService}
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object LockingBuilder {
  def buildDynamoLockDao(config: Config)(implicit ec: ExecutionContext): DynamoLockDao =
    new DynamoLockDao(
      client = DynamoBuilder.buildDynamoClient(config),
      config = DynamoLockDaoConfig(
        dynamoConfig = DynamoBuilder.buildDynamoConfig(config, namespace = "locking"),
        expiryTime = Duration.ofSeconds(
          config.getOrElse[Int]("locking.expiryTime")(default = 180)
        )
      )
    )

  def buildDynamoLockingService(config: Config)(implicit ec: ExecutionContext): DynamoLockingService = {
    implicit val dynamoLockDao: DynamoLockDao = buildDynamoLockDao(config)

    new DynamoLockingService()
  }
}
