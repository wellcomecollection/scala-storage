package uk.ac.wellcome.storage.store

import java.io.InputStream

import uk.ac.wellcome.storage.streaming.FiniteStream

trait StreamingStore[Ident, IS <: InputStream with FiniteStream] extends Store[Ident, IS]
