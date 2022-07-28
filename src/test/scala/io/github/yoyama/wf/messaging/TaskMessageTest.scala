package io.github.yoyama.wf.messaging

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.Charset

class TaskMessageTest  extends AnyFlatSpec {
  val ser = new TaskMessageSerializer
  val deser = new TaskMessageDeserializer

  "deserializer" should "work" in {
    //id:Int, runId:Int, mType:Int, body:String
    val data = """{ "id": 1, "runId": 31, "taskId":78, "mType": 2, "body": "{ \"aa\": \"bbbb\" }"}""".getBytes(Charset.forName("UTF-8"))
    val message = deser.deserialize("aaa", data)
    println(message)
    assert(message.id == 1)
    assert(message.runId == 31)
    assert(message.taskId == 78)
    assert(message.mType == 2)
    assert(message.body == "{ \"aa\": \"bbbb\" }")
  }

  "serializer" should "work" in {
    val message = TaskMessage(1, 31, 78, 2, "{ \"aa\": \"bbbb\" }")
    val bytes = ser.serialize("aaa", message)
    val message2 = deser.deserialize("aaa", bytes)
    assert(message == message2)
  }


}
