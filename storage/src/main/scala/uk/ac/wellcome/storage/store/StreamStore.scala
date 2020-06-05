package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage.streaming.InputStreamWithLength

trait StreamStore[Ident] extends Store[Ident, InputStreamWithLength]
