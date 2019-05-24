package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.ScanamoError
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.{KeyEquals, UniqueKey}
import com.gu.scanamo.syntax._
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter}
import uk.ac.wellcome.storage.{ConditionalUpdateDao, DaoError, ReadError, WriteError}

class DynamoConditionalUpdateDao[Ident, T](
  underlying: DynamoDao[Ident, T]
)(
  implicit
  idGetter: IdGetter[T],
  versionGetter: VersionGetter[T]
) extends ConditionalUpdateDao[Ident, T] {
  override def get(id: Ident): DaoGetResult = underlying.get(id)

  override def put(t: T): DaoPutResult = underlying.executeWriteOps(
    id = idGetter.id(t),
    ops = buildConditionalUpdate(t)
  ).map { _ => () }

  private def buildConditionalUpdate(
    t: T): ScanamoOps[Either[ScanamoError, T]] =
    underlying
      .buildUpdate(t)
      .map { updateExpression =>
        underlying.table
          .given(
            not(attributeExists('id)) or
              (attributeExists('id) and 'version < versionGetter.version(t))
          )
          .update(
            UniqueKey(KeyEquals('id, idGetter.id(t))),
            updateExpression
          )
      }
      .getOrElse(
        // Everything that gets passed into updateBuilder should have an "id"
        // and a "version" field, and the compiler enforces this with the
        // implicit IdGetter[T] and VersionGetter[T].
        //
        // generateUpdateExpression returns None only if the record only
        // contains an "id" field, so we should always get Some(ops) out
        // of this function.
        //
        throw new Exception(
          "Trying to update a record that only has an id: this should be impossible!")
      )
}

object DynamoConditionalUpdateDao {
  def apply[Ident, T](
    dynamoClient: AmazonDynamoDB,
    dynamoConfig: DynamoConfig
  )(
    implicit
    evidence: DynamoFormat[T],
    idGetter: IdGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T],
    versionGetter: VersionGetter[T]
  ): DynamoConditionalUpdateDao[Ident, T] =
    new DynamoConditionalUpdateDao(
      new DynamoDao[Ident, T](
        dynamoClient = dynamoClient,
        dynamoConfig = dynamoConfig
      )
    )
}
