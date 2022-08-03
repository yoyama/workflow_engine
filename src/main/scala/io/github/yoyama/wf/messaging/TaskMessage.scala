package io.github.yoyama.wf.messaging

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{JsResult, JsValue, Json, Reads, Writes}

import java.nio.charset.Charset
import java.util
import scala.util.control.Exception.catching

/**
 * // Command to TaskProcessor
 *mType:   1: ready to run
          *2: cancel
         *11: monitor 
 *
 */
case class TaskMessage(id:Int, runId:Int, taskId:Int, mType:TaskMessageType, body:String)

enum TaskMessageType(val value:Int) {
  case REQ_START extends TaskMessageType(1)
  case REQ_CANCEL extends TaskMessageType(2)
  case REQ_MONITOR extends TaskMessageType(11)
}

object TaskMessageType {
  def apply(v:Int):TaskMessageType = v match {
    case 1 => REQ_START
    case 2 => REQ_CANCEL
    case 11 => REQ_MONITOR
  }
}


class TaskMessageSerializer() extends Serializer[TaskMessage] {
  implicit val writes:Writes[TaskMessage] = Json.writes[TaskMessage]
  implicit val writesMessageType:Writes[TaskMessageType] = (o: TaskMessageType) => Json.toJson(o.value)

  override def serialize(topic: String, data: TaskMessage): Array[Byte] = {
    Json.toJson(data).toString.getBytes(Charset.forName("UTF-8"))
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }
}

object TaskMessageSerializer {
  def apply():TaskMessageSerializer = new TaskMessageSerializer()
}

class TaskMessageDeserializer() extends Deserializer[TaskMessage] {
  implicit val reads:Reads[TaskMessage] = Json.reads[TaskMessage]
  implicit val readMessageType: Reads[TaskMessageType] = (json: JsValue) => JsResult.fromTry(
    catching(classOf[RuntimeException]) withTry TaskMessageType.apply(json.as[Int])
  )

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): TaskMessage = {
    Json.fromJson[TaskMessage](Json.parse(data)) match {
      case result if result.isSuccess => result.get
      case result => throw new RuntimeException(result.toString)
    }
  }
}

object TaskMessageDeserializer {
  def apply():TaskMessageDeserializer = new TaskMessageDeserializer()
}
