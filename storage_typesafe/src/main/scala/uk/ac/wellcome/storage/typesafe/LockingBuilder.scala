package uk.ac.wellcome.storage.typesafe

import java.time.Duration

import com.typesafe.config.Config
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.storage.locking.dynamo.{DynamoLockDao, DynamoLockDaoConfig, DynamoLockingService}
import uk.ac.wellcome.storage.locking.{DynamoLockDaoConfig, DynamoLockingService}
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object LockingBuilder {
  def buildDynamoLockDao(config: Config)(
    implicit ec: ExecutionContext): DynamoLockDao =
    new DynamoLockDao(
      client = DynamoBuilder.buildDynamoClient(config),
      config = DynamoLockDaoConfig(
        dynamoConfig =
          DynamoBuilder.buildDynamoConfig(config, namespace = "locking"),
        expiryTime = Duration.ofSeconds(
          config.getOrElse[Int]("locking.expiryTime")(default = 180)
        )
      )
    )

  def buildDynamoLockingService[Out, OutMonad[_]](config: Config)(
    implicit ec: ExecutionContext): DynamoLockingService[Out, OutMonad] = {
    implicit val dynamoLockDao: DynamoLockDao = buildDynamoLockDao(config)

    new DynamoLockingService()
  }
}
