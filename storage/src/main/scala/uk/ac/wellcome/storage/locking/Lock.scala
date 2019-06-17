package uk.ac.wellcome.storage.locking

trait Lock[Ident, ContextId] {
  val id: Ident
  val contextId: ContextId
}
