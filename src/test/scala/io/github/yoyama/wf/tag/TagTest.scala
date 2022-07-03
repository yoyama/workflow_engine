package io.github.yoyama.wf.tag

import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Success

class TagTest extends AnyFlatSpec{
  "create from string" should "work" in {
    val tag = Tag.from("""{ "key1": "abcd" }""")
    assert(Success("abcd") == tag.get.getString("key1"))
  }

  "getString" should "work" in {

  }
}
