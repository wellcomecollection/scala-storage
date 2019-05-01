package uk.ac.wellcome.storage.locking

import java.time.Duration

import uk.ac.wellcome.storage.dynamo.DynamoConfig

case class DynamoLockDaoConfig(
  dynamoConfig: DynamoConfig,
  duration: Duration = Duration.ofSeconds(180)
)
