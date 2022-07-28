package io.github.yoyama.wf.messaging

import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.scalatest.flatspec.AnyFlatSpec

class KafkaQueueSenderTest extends AnyFlatSpec {
  val topic = sys.env.getOrElse("TEST_TOPIC", "workflow-proc")
  val senderConfig = KafkaQueueSenderConfig(bootstrap = "localhost:9092")
  implicit val stringSerializer: Serializer[String] = new StringSerializer

  "send" should "work" in {
    val sender = new KafkaQueueSender[String](senderConfig, topic)
    val ret = sender.send("aa", "aaaaabbbbbbbbbbbbbb")
    println(ret.get.topic())
    println(ret.get.offset())
    println(ret.get.partition())
    println(ret)
  }
}
