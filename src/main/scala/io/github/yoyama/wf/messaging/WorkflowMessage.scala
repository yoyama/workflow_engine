package io.github.yoyama.wf.messaging

import com.fasterxml.jackson.databind.ObjectMapper

import java.nio.charset.Charset
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.common.header.Headers
import play.api.libs.json.{JsObject, JsPath, JsResult, JsString, JsValue, Json, Reads, Writes}
import play.api.libs.functional.syntax.*
import play.api.libs.json.Reads.*
import play.api.libs.json.Writes.*

import scala.util.control.Exception.*


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
case class WorkflowMessage(id:Int, runId:Int, mType:WorkflowMessageType, body:String)

enum WorkflowMessageType(val value:Int) {
  case REQ_START extends WorkflowMessageType(1)
  case REQ_CANCEL extends WorkflowMessageType(2)
  case NOTIFY_TASK_START extends WorkflowMessageType(101)
  case NOTIFY_TASK_RUNNING extends WorkflowMessageType(102)
  case NOTIFY_TASK_STOP_SUCCESS extends WorkflowMessageType(103)
  case NOTIFY_TASK_STOP_FAIL extends WorkflowMessageType(104)
}

object WorkflowMessageType {
  def apply(v:Int):WorkflowMessageType = v match {
    case 1 => REQ_START
    case 2 => REQ_CANCEL
    case 101 => NOTIFY_TASK_START
    case 102 => NOTIFY_TASK_RUNNING
    case 103 => NOTIFY_TASK_STOP_SUCCESS
    case 104 => NOTIFY_TASK_STOP_FAIL
    case _ => throw new RuntimeException(s"Invalid workflow message type. value:${v}")
  }
}


class WorkflowMessageSerializer() extends Serializer[WorkflowMessage] {
  implicit val writes:Writes[WorkflowMessage] = Json.writes[WorkflowMessage]
  implicit val writesMessageType:Writes[WorkflowMessageType] = (o: WorkflowMessageType) => Json.toJson(o.value)

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
  implicit val readMessageType: Reads[WorkflowMessageType] = (json: JsValue) => JsResult.fromTry(
    catching(classOf[RuntimeException]) withTry {
      WorkflowMessageType.apply(json.as[Int])
    }
  )

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
