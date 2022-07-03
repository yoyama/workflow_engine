package io.github.yoyama.wf.tag
import play.api.libs.json.{JsValue, Json}

import java.time.Instant
import scala.util.control.Exception.*
import scala.util.Try


case class TagKeyNotFoundExeception(key:String) extends RuntimeException(s"key: ${key} not found")
/**
 *  Key-Value tag
 *  Key: String
 *  Value: String | Long | Instant
 */
class Tag(val json: JsValue) {

  def getString(key:String):Try[String] = {
    val ret = catching(classOf[RuntimeException]) either {
      (json \ key).get.as[String]
    }
    ret.toTry
  }
  def getLong(key:String): Try[Long] = ???
  def getInstant(key:String): Try[Instant] = ???
  def exist(key:String):Boolean = ???
  def getKeys():Seq[String] = { ??? }
  override def toString: String = super.toString
}

object Tag {
  def apply(json: JsValue):Tag = new Tag(json)
  
  def from(jsonStr: String): Try[Tag] = {
    val ret = catching(classOf[RuntimeException]) either {
      new Tag(Json.parse(jsonStr))
    }
    ret.toTry
  }
}
