package io.github.yoyama.wf.tag
import play.api.libs.json.{JsValue, JsObject, Json}
import play.api.libs.json.{Writes, Reads}
import java.time.Instant
import scala.util.control.Exception.*
import scala.util.Try


case class TagKeyNotFoundExeception(key:String) extends RuntimeException(s"key: ${key} not found")
/**
 *  Key-Value tag
 *  Key: String
 *  Value: String | Long | Instant
 */
class Tag(val json: JsObject) {

  def getString(key:String):Try[String] = getValue[String](key)
  def getLong(key:String): Try[Long] = getValue[Long](key)
  def getInstant(key:String): Try[Instant] = getValue[Instant](key)

  private def getValue[T](key:String)(implicit read:Reads[T]): Try[T] = {
    val ret = catching(classOf[RuntimeException]) either {
      (json \ key).get.as[T]
    }
    ret.toTry

  }

  def exist(key:String):Boolean = (json \ key).isDefined
  def keys():Set[String] = {
    json.keys.toSet
  }

  override def toString: String = {
    json.toString
  }
}

object Tag {
  def apply(json: JsObject):Tag = new Tag(json)
  def apply():Tag = Tag.from("{}").get

  def from(jsonStr: Option[String]): Try[Tag] = {
    from(jsonStr.getOrElse("{}"))
  }

  def from(jsonStr: String): Try[Tag] = {
    val ret = catching(classOf[RuntimeException]) either {
      new Tag(Json.parse(jsonStr).asInstanceOf[JsObject])
    }
    ret.toTry
  }
}
