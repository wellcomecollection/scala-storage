package uk.ac.wellcome.storage.dynamo

case class DynamoNonFatalError(e: Throwable)
    extends Exception(e.getMessage)
