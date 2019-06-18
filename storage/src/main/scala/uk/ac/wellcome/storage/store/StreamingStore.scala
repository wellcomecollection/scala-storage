package uk.ac.wellcome.storage.store

import java.io.InputStream

import uk.ac.wellcome.storage.streaming.HasLength

trait StreamingStore[Ident, IS <: InputStream with HasLength] extends Store[Ident, IS]
