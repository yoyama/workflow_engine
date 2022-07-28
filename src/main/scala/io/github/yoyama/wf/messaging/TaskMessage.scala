package io.github.yoyama.wf.messaging

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Json, Reads, Writes}

import java.nio.charset.Charset
import java.util

/**
 * // Command to TaskProcessor
 *mType:   1: ready to run
          *2: cancel
         *11: monitor 
 *
 */
case class TaskMessage(id:Int, runId:Int, taskId:Int, mType:Int, body:String)

class TaskMessageSerializer() extends Serializer[TaskMessage] {
  implicit val writes:Writes[TaskMessage] = Json.writes[TaskMessage]

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
