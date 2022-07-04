package io.github.yoyama.wf.tag

import java.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import scala.util.Success

class TagTest extends AnyFlatSpec{
  val testJsonText =
    """{
      |   "key1": "abcd",
      |   "key2":  123456,
      |   "key3":  "2022-06-22T12:42:04+09:00"
      | }
      |""".stripMargin

  "create from string" should "work" in {
    val tag = Tag.from(testJsonText)
    assert(Success("abcd") == tag.get.getString("key1"))
  }

  "create empty tag" should "work" in {
    val tag = Tag()
  }

  "getLong" should "work" in {
    val tag = Tag.from(testJsonText)
    assert(Success(123456L) == tag.get.getLong("key2"))
    assert(tag.get.getLong("key1").isFailure)
  }

  "getInstant" should "work" in {
    val tag = Tag.from(testJsonText)
    val target = Instant.parse( "2022-06-22T03:42:04Z" )
    assert(Success(target) == tag.get.getInstant("key3"))
  }

  "keys" should "work" in {
    val tag = Tag.from(testJsonText)
    assert(Set("key1", "key2", "key3") == tag.get.keys())
  }
}
