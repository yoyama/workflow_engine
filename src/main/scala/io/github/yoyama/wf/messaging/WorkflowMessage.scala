package io.github.yoyama.wf.messaging

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.header.Headers
import play.api.libs.json.{JsObject, JsResult, JsValue, Json, Reads, Writes}

import java.nio.charset.Charset
import java.util

// From Task to Workflow

/**
 * 
 * @param id message unique id
 * @param workflowId id of the workflow the source
 * @param taskId id of the task of the source
 * @param mType message type
 *              1: request to start workflow
 *              2: request to cancel workflow
 *              101:notification on task started
 *              102:notification on task is running
 *              103:notification on task stopped successfully
 *              104 notification on task stopped with failures
 *
 * @param body message body(JSON)
 */
case class WorkflowMessage(id:Int, runId:Int, mType:Int, body:String)

class WorkflowMessageSerializer() extends Serializer[WorkflowMessage] {
  implicit val writes:Writes[WorkflowMessage] = Json.writes[WorkflowMessage]

  override def serialize(topic: String, data: WorkflowMessage): Array[Byte] = {
    Json.toJson(data).toString.getBytes(Charset.forName("UTF-8"))
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }
}

object WorkflowMessageSerializer {
  def apply():WorkflowMessageSerializer = new WorkflowMessageSerializer()
}

class WorkflowMessageDeserializer() extends Deserializer[WorkflowMessage] {
  implicit val reads:Reads[WorkflowMessage] = Json.reads[WorkflowMessage]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): WorkflowMessage = {
    Json.fromJson[WorkflowMessage](Json.parse(data)) match {
      case result if result.isSuccess => result.get
      case result => throw new RuntimeException(result.toString)
    }
  }
}

object WorkflowMessageDeserializer {
  def apply():WorkflowMessageDeserializer = new WorkflowMessageDeserializer()
}
