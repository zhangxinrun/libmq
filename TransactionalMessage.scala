/**
  * Copyright 2019 Loopring Foundation
  * Author: wxl@loopring.org (Wu XiaoLu)
  */

package io.lightcone.utils.mq

import io.lightcone.data.mq.{MQPublishRecord, MQPublishStatus}
import io.lightcone.data.types.ErrorCode
import io.lightcone.utils.mq.persistence.DatabaseModule
import org.slf4s.Logging
import scalapb.GeneratedMessage
import slick.dbio.{DBIOAction, NoStream}

import scala.concurrent.{ExecutionContext, Future}

trait TransactionalMessage extends Logging {
  implicit val messagePersist: DatabaseModule
  implicit val ec: ExecutionContext
  val byteProducer: ByteArrayProducer

  private val DEFAULT_RETRY_TIMES: Int = 100

  def runWithMessageSend[S, K <: GeneratedMessage, T](
      req: Option[S],
      messageBuilder: (Option[S], Option[T]) => K,
      topic: String,
      partitionKey: Option[String],
      partition: Option[Int],
      action: DBIOAction[T, NoStream, _]
    ): Future[ErrorCode] = {

    log.info("====> send message")
    log.info(s"TOPIC : $topic")

    val mqId = ProducerFactory.generateMessageId(topic)

    val now = getTimeSeconds
    val r = MQPublishRecord(
      messageId = mqId,
      status = MQPublishStatus.NEW,
      topic = topic,
      willTry = DEFAULT_RETRY_TIMES,
      createdAt = now,
      updatedAt = now
    )

    for {
      ru <- messagePersist.mqPublishRecordDal.runWithMessagePersist(
        r,
        action,
        req,
        messageBuilder
      )
      _ <- {
        byteProducer
          .sendByte(
            ru.topic,
            ru.payload.toByteArray,
            partition,
            partitionKey
          )
          .map { sendRst =>
            log.info(s"send msg success $topic, ${sendRst.offset()}")
          }
      }
      _ <- messagePersist.mqPublishRecordDal.updateSendSucc(mqId)
    } yield ErrorCode.ERR_NONE
  }
}
