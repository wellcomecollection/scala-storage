package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

import scala.concurrent.{ExecutionContext, Future}

class DynamoVersionedDao[T](
  val underlying: DynamoConditionalUpdateDao[T]
)(implicit
  ec: ExecutionContext,
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T])
    extends Logging
    with VersionedDao[T] {

  @deprecated("Use put() instead of updateRecord()", since = "2019-05-10")
  def updateRecord[R](record: T): Future[T] =
    Future
      .fromTry { put(record) }
      .recover {
        case t: ProvisionedThroughputExceededException =>
          throw DynamoNonFatalError(t)
      }

  @deprecated("Use get() instead of getRecord()", since = "2019-05-10")
  def getRecord[R](id: String): Future[Option[T]] =
    Future
      .fromTry { get(id) }
      .recover {
        case t: ProvisionedThroughputExceededException =>
          throw DynamoNonFatalError(t)
      }
}
