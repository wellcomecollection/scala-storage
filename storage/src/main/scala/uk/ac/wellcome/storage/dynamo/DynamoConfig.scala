package uk.ac.wellcome.storage.dynamo

import javax.naming.ConfigurationException

case class DynamoConfig(tableName: String, maybeIndexName: Option[String] = None) {
  def indexName: String = maybeIndexName.getOrElse(
    throw new ConfigurationException(
      "Tried to look up the index, but no index is configured!"
    )
  )
}

case object DynamoConfig {
  def apply(tableName: String, indexName: String): DynamoConfig =
    DynamoConfig(
      tableName = tableName,
      maybeIndexName = Some(indexName)
    )
}
