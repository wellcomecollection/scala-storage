package uk.ac.wellcome.storage

trait Lock[Ident, ContextId] {
  val id: Ident
  val contextId: ContextId
}
