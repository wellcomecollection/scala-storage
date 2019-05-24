package uk.ac.wellcome.storage.typesafe

import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.config.models.AWSClientConfig
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.s3._
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.typesafe.config.builders.AWSClientConfigBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object S3Builder extends AWSClientConfigBuilder {
  private def buildS3Client(awsClientConfig: AWSClientConfig): AmazonS3 =
    S3ClientFactory.create(
      region = awsClientConfig.region,
      endpoint = awsClientConfig.endpoint.getOrElse(""),
      accessKey = awsClientConfig.accessKey.getOrElse(""),
      secretKey = awsClientConfig.secretKey.getOrElse("")
    )

  def buildS3Client(config: Config): AmazonS3 =
    buildS3Client(
      awsClientConfig = buildAWSClientConfig(config, namespace = "s3")
    )

  def buildS3Config(config: Config, namespace: String = ""): S3Config = {
    val bucketName = config
      .required[String](s"aws.$namespace.s3.bucketName")

    S3Config(
      bucketName = bucketName
    )
  }

  def buildObjectStore[T](config: Config)(implicit codec: Codec[T]): ObjectStore[T] = {
    implicit val storageBackend: S3StorageBackend = new S3StorageBackend(
      s3Client = buildS3Client(config)
    )

    ObjectStore[T]
  }

  def buildS3PrefixCopier(config: Config): S3PrefixCopier =
    S3PrefixCopier(s3Client = buildS3Client(config))
}
