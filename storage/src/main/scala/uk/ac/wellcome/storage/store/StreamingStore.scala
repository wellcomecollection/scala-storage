package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage.streaming.FiniteInputStream

trait StreamingStore[Ident, IS <: FiniteInputStream] extends Store[Ident, IS]
