package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.DynamoFormat
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{
  IdGetter,
  VersionGetter,
  VersionUpdater
}

class DynamoVersionedDao[Ident, T](
  val underlying: DynamoConditionalUpdateDao[Ident, T]
)(implicit
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T])
    extends Logging
    with VersionedDao[Ident, T]

object DynamoVersionedDao {
  def apply[Ident, T](
    dynamoClient: AmazonDynamoDB,
    dynamoConfig: DynamoConfig
  )(
    implicit
    evidence: DynamoFormat[T],
    idGetter: IdGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T],
    versionGetter: VersionGetter[T],
    versionUpdater: VersionUpdater[T]
  ): DynamoVersionedDao[Ident, T] =
    new DynamoVersionedDao(
      DynamoConditionalUpdateDao[Ident, T](
        dynamoClient = dynamoClient,
        dynamoConfig = dynamoConfig
      )
    )
}
