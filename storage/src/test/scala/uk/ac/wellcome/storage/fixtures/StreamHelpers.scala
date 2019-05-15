package uk.ac.wellcome.storage.fixtures

import java.io.InputStream

import org.apache.commons.io.IOUtils

import scala.io.Source

trait StreamHelpers {
  def toStream(s: String): InputStream =
    IOUtils.toInputStream(s, "UTF-8")

  def fromStream(is: InputStream): String =
    Source.fromInputStream(is).mkString
}
