/**
  * Copyright 2019 Loopring Foundation
  * Author: wxl@loopring.org (Wu XiaoLu)
  */

package io.lightcone.utils.mq

import java.util
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.lightcone.data.mq.MQTopic
import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import scalapb._
import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class Consumer[T](
    kafkaConsumer: JKafkaConsumer[String, T],
    handler: MessageHandler[T])

case class SubscribeConf(
    groupName: String,
    isSync: Boolean,
    handlerTimeout: Option[Duration],
    ifLatest: Boolean = false) // false => "earliest", true => "latest"

object ConsumerFactory {

  private val CONSUMER_IDENTIFY_KEY = "c_i_k"
  private val UNDERLINE_DELIMITER = "_"

  private val conf: Config = confirmIPByEnv(
    ConfigFactory.load.getConfig("kafka.consumer")
  )

  private var registerMap: Map[String, Consumer[_ <: Any]] =
    Map.empty

  private var runningConsumer: List[ConsumerRunnable[_ <: Any]] =
    List.empty

  def subscribe[T <: GeneratedMessage with scalapb.Message[T]](
      topic: MQTopic,
      handler: MessageHandler[T],
      sc: SubscribeConf
    )(
      implicit
      companion: GeneratedMessageCompanion[T]
    ): Unit =
    subscribe[T](
      topic.name,
      handler,
      sc,
      new ProtoDeserializer[T]()
    )

  def subscribe[T <: GeneratedMessage with scalapb.Message[T]](
      topic: String,
      handler: MessageHandler[T],
      sc: SubscribeConf
    )(
      implicit
      companion: GeneratedMessageCompanion[T]
    ): Unit =
    subscribe[T](
      topic,
      handler,
      sc,
      new ProtoDeserializer[T]()
    )

  def subscribeString(
      topic: String,
      handler: MessageHandler[String],
      sc: SubscribeConf
    ): Unit =
    subscribe[String](
      topic,
      handler,
      sc,
      new StringDeserializer
    )

  def subscribeWithFixedConsumerIdentifyKey[
      T <: GeneratedMessage with scalapb.Message[T]
    ](topic: MQTopic,
      handler: MessageHandler[T],
      sc: SubscribeConf,
      consumerIdentifyKey: String
    )(
      implicit
      companion: GeneratedMessageCompanion[T]
    ): Unit =
    subscribe[T](
      topic.name,
      handler,
      sc,
      new ProtoDeserializer[T](),
      Some(consumerIdentifyKey)
    )

  def subscribe[T](
      topic: String,
      handler: MessageHandler[T],
      sc: SubscribeConf,
      deserializer: Deserializer[T],
      consumerIdentifyKey: Option[String] = None
    ): Unit = {
    val clientId = String
      .join(
        UNDERLINE_DELIMITER,
        topic,
        sc.groupName,
        consumerIdentifyKey.getOrElse(CONSUMER_IDENTIFY_KEY)
      )
      .toLowerCase
    val idAppended = conf
      .withValue("group.id", ConfigValueFactory.fromAnyRef(sc.groupName))

    val withAutoResetConf =
      if (sc.ifLatest)
        idAppended.withValue(
          "auto.offset.reset",
          ConfigValueFactory.fromAnyRef("latest")
        )
      else
        idAppended

    val c = KafkaConsumer(
      Conf(
        withAutoResetConf,
        new StringDeserializer(),
        deserializer
      )
    )
    val consumer = Consumer(c, handler)
    if (!registerMap.isDefinedAt(clientId)) {
      registerMap += (clientId -> consumer)
      val topics = new util.LinkedList[String]()
      topics.add(topic)
      startPoll(
        topics,
        withAutoResetConf,
        handler,
        SyncConfig(sc.isSync, sc.handlerTimeout),
        deserializer
      )

    } else {
      throw new IllegalArgumentException(s"$topic had register")
    }
  }

  private def startPoll[T](
      topics: util.List[String],
      conf: Config,
      handler: MessageHandler[T],
      syncConfig: SyncConfig,
      deserializer: Deserializer[T]
    ): Unit = {
    val c: JKafkaConsumer[String, T] =
      KafkaConsumer(
        Conf(conf, new StringDeserializer(), deserializer)
      )

    val cr = new ConsumerRunnable[T](topics, handler, syncConfig, c)
    runningConsumer = runningConsumer.+:(cr)
    new Thread(cr).start()
  }

  def close(): Unit = {
    runningConsumer.foreach(_.shutdown())
    runningConsumer = List.empty
    registerMap = Map.empty
  }

}

trait MessageHandler[T] {
  def handle(record: ConsumerRecord[String, T]): Future[Boolean]
}

class DemoMessageHandler[T <: GeneratedMessage] extends MessageHandler[T] {

  override def handle(record: ConsumerRecord[String, T]): Future[Boolean] = {
    Future.successful(true)
  }
}

private class ProtoDeserializer[T <: GeneratedMessage with Message[T]](
    implicit
    companion: GeneratedMessageCompanion[T])
    extends Deserializer[T] {

  override def configure(
      configs: util.Map[String, _],
      isKey: Boolean
    ): Unit = {}

  override def close(): Unit = {}

  override def deserialize(
      topic: String,
      data: Array[Byte]
    ): T = {
    companion.parseFrom(data)
  }
}
