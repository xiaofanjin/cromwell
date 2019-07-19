package cromwell.backend.google.pipelines.v2alpha1

import java.nio.file.Paths

import cromwell.backend.google.pipelines.common.PipelinesApiFileInput
import cromwell.core.path.DefaultPathBuilder
import org.scalatest.{FlatSpec, Matchers}

class PipelinesParameterConversionsSpec extends FlatSpec with Matchers {
  behavior of "PipelinesParameterConversions"

  it should "group files by bucket" in {

    def makeInput(bucket: String, name: String): PipelinesApiFileInput = {
      val path = DefaultPathBuilder.build(Paths.get("foo1"))
      PipelinesApiFileInput(
        name = name,
        cloudPath = DefaultPathBuilder.build(Paths.get(s"gs://$bucket/$name")),
        relativeHostPath = DefaultPathBuilder.build(Paths.get(s"$bucket/$name")),
        mount = null
      )
    }

    val inputs = List(
      List("")

    )

  }

}
