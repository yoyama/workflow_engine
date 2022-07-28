package io.github.yoyama.wf.messaging

import io.github.yoyama.utils.FutureHelper.*

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}

import java.time.{Duration => JavaDuration}
import java.util.Properties
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*
import scala.jdk.FutureConverters.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

trait QueueSender[M,R] {
  def send(key:String, message:M):Try[R]
}

trait QueueReceiver[M,R] {
  def receive(): Try[Seq[M]]
  def commitReceive():Try[R]
}


case class KafkaQueueSenderConfig(bootstrap:String)

class KafkaQueueSender[M](val config: KafkaQueueSenderConfig, val topic:String)(implicit val valueSerializer:Serializer[M]) extends QueueSender[M, RecordMetadata] {
  val producer = init()
  def send(key:String, message:M):Try[RecordMetadata] = {
    Try {
      val rec = new ProducerRecord[String, M](topic, key, message)
      val f = producer.send(rec).toFuture()
      Await.result (f, Duration.Inf)
    }
  }

  def init(): KafkaProducer[String,M] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrap)
    new KafkaProducer(properties, new StringSerializer(), valueSerializer)
  }
}

case class KafkaQueueReceiverConfig(bootstrap:String, consumerGroup:String, maxPollRecords:Int = 10)

class KafkaQueueReceiver[M](val config: KafkaQueueReceiverConfig, val topic:String)(implicit val valueDeserializer:Deserializer[M])  extends QueueReceiver[M, Unit] {
  val consumer = init()

  def receive(): Try[Seq[M]] = {
    Try {
      val recs = consumer.poll(10.seconds.toJava).asScala
      recs.toSeq.map(r => r.value())
    }
  }

  def commitReceive():Try[Unit] = {
    Try {
      consumer.commitSync()
    }
  }

  def init(): KafkaConsumer[String,M] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrap)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroup)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // default "latest"
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords)

    val consumer = new KafkaConsumer(properties, new StringDeserializer(), valueDeserializer)
    consumer.subscribe(List(topic).asJava)
    consumer
  }
}

class WorkflowQueueSender(config: KafkaQueueSenderConfig, topic:String)
  extends KafkaQueueSender[WorkflowMessage](config, topic)(WorkflowMessageSerializer())

class WorkflowQueueReceiver(config: KafkaQueueReceiverConfig, topic:String)
  extends KafkaQueueReceiver[WorkflowMessage](config, topic)(WorkflowMessageDeserializer())