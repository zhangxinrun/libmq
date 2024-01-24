/**
  * Copyright 2019 Loopring Foundation
  * Author: wxl@loopring.org (Wu XiaoLu)
  */

package io.lightcone.utils.mq

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord, KafkaSerializer}
import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.KafkaProducerRecord.Destination
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.lightcone.data.mq.MQTopic
import org.apache.kafka.common.serialization.{
  ByteArraySerializer,
  StringSerializer
}
import scalapb.GeneratedMessage
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4s.Logging

import scala.concurrent.{ExecutionContext, Future}

trait Producer[T] extends Logging {

  def p: KafkaProducer[String, T]
  def topic: String

  def send(
      msg: T,
      partition: Option[Int] = None,
      partitionKey: Option[String] = None
    ): Future[RecordMetadata] = {
    p.send(
      KafkaProducerRecord(
        Destination(topic, partition),
        partitionKey,
        msg
      )
    )
  }

  private val DEFAULT_PARTITION: Int = 0

  def sendWithFixedPartition(
      msg: T,
      partition: Int = DEFAULT_PARTITION
    ): Future[RecordMetadata] =
    p.send(
      KafkaProducerRecord(
        Destination(topic, Some(partition)),
        None,
        msg
      )
    )

  // for message transaction
  def beginTransaction(): Unit = {
    p.initTransactions
    p.beginTransaction
  }

  def commitTransaction(): Unit = p.commitTransaction
  def abortTransaction(): Unit = p.abortTransaction
}

case class ByteArrayProducer(
    override val topic: String,
    override val p: KafkaProducer[String, Array[Byte]])
    extends Producer[Array[Byte]] {

  def sendByte(
      topic: String,
      msg: Array[Byte],
      partition: Option[Int] = None,
      partitionKey: Option[String] = None
    ): Future[RecordMetadata] = {
    p.send(
      KafkaProducerRecord(
        Destination(topic, partition),
        partitionKey,
        msg
      )
    )
  }
}

case class ProtoProducer[T <: GeneratedMessage](
    override val topic: String,
    override val p: KafkaProducer[String, T])
    extends Producer[T]

object ProducerFactory {

  private var m: Map[String, ProtoProducer[GeneratedMessage]] = Map.empty
  private val serializer = KafkaSerializer(protoSerializer)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val conf = confirmIPByEnv(
    ConfigFactory.load.getConfig("kafka.producer")
  )

  private lazy val byteArrayProducer: KafkaProducer[String, Array[Byte]] = {
    val kc = Conf(conf, new StringSerializer(), new ByteArraySerializer())
    KafkaProducer(kc)
  }

  def getProtoProducer(topic: MQTopic): ProtoProducer[GeneratedMessage] =
    getProtoProducer(topic.name)

  def getProtoProducer(topic: String): ProtoProducer[GeneratedMessage] =
    this.synchronized {
      m.get(topic) match {
        case None =>
          val newInstance =
            ProtoProducer[GeneratedMessage](topic, instanceProto(topic))
          m += (topic -> newInstance)
          newInstance
        case Some(p) => p
      }
    }

  def getByteProducer: ByteArrayProducer =
    ByteArrayProducer("", byteArrayProducer)

  private def protoSerializer[T <: GeneratedMessage](msg: T): Array[Byte] =
    msg.toByteArray

  private def instanceProto(
      topic: String
    ): KafkaProducer[String, GeneratedMessage] =
    this.synchronized {
      val withTopic = conf.withValue(
        "client.id",
        ConfigValueFactory.fromAnyRef(
          s"${topic}_Producer_$getTimeSeconds"
        )
      )

      val kc = Conf(withTopic, new StringSerializer(), serializer)
      KafkaProducer(kc)
    }

  def generateMessageId(topic: String): String = {
    topic + getTimeSeconds + randomStr
  }
}

final case class TrySendFailedException(msg: String) extends Exception(msg)
