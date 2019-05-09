package uk.ac.wellcome.storage.s3

import java.net.URI

import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Success

class S3UrlsTest extends FunSpec with Matchers {
  val examples = Table(
    (
      "s3://bucket/key.txt",
      "bucket", "key.txt"
    ),
    (
      "s3://bucket/key/in/nested/directories.txt",
      "bucket", "key/in/nested/directories.txt"
    )
  )

  it("decodes from a URI to an ObjectLocation") {
    forAll(examples) { (url: String, namespace: String, key: String) =>
      S3Urls.decode(new URI(url)) shouldBe Success(ObjectLocation(namespace, key))
    }
  }

  it("encodes an ObjectLocation as a URI") {
    forAll(examples) { (url: String, namespace: String, key: String) =>
      S3Urls.encode(ObjectLocation(namespace, key)) shouldBe new URI(url)
    }
  }

  it("rejects a URI which doesn't start with S3") {
    val result = S3Urls.decode(new URI("https://example.org/hello"))
    result.isFailure shouldBe true
    result.failed.get shouldBe a[IllegalArgumentException]
  }
}
