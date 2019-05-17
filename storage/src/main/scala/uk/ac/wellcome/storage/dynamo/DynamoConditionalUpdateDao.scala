package uk.ac.wellcome.storage.dynamo

import com.gu.scanamo.error.ScanamoError
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.query.{KeyEquals, UniqueKey}
import com.gu.scanamo.syntax._
import uk.ac.wellcome.storage.ConditionalUpdateDao
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter}

import scala.util.Try

class DynamoConditionalUpdateDao[T](
  underlying: DynamoDao[T]
)(
  implicit
  idGetter: IdGetter[T],
  versionGetter: VersionGetter[T]
) extends ConditionalUpdateDao[String, T] {
  override def get(id: String): Try[Option[T]] = underlying.get(id)

  override def put(t: T): Try[T] = underlying.executeOps(
    id = idGetter.id(t),
    ops = buildConditionalUpdate(t)
  )

  private def buildConditionalUpdate(t: T): ScanamoOps[Either[ScanamoError, T]] =
    underlying.buildUpdate(t)
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
