package uk.ac.wellcome.storage

import org.scanamo.DynamoFormat

trait Identifiable[Id] {
  val id: Id

  def asKey: IdentityKey = IdentityKey(id.toString)
}

case class IdentityKey(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

case object IdentityKey {
  implicit def format: DynamoFormat[IdentityKey] =
    DynamoFormat.iso[IdentityKey, String](
      IdentityKey(_)
    )(
      _.underlying
    )
}

case class Identified[Id, T](id: Id, identifiedT: T) extends Identifiable[Id]

case class Version[Id, V](id: Id, version: V) extends Identifiable[Id] {

  override def asKey: IdentityKey = IdentityKey(s"$id/$version")
}
