/**
  * Copyright 2019 Loopring Foundation
  * Author: wxl@loopring.org (Wu XiaoLu)
  */

package io.lightcone.utils.mq

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.{
  CommitFailedException,
  ConsumerRecord,
  OffsetAndMetadata,
  KafkaConsumer => JKafkaConsumer
}
import org.apache.kafka.common.TopicPartition
import org.slf4s.Logging
import io.lightcone.utils.thread.MonitoredThreadPoolExecutor
import java.time.{Duration => JDuration}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.Breaks.{break, breakable}

class ConsumerRunnable[T](
    val topics: java.util.List[String],
    val handler: MessageHandler[T],
    val syncConfig: SyncConfig,
    val c: JKafkaConsumer[String, T])
    extends Runnable
    with Logging {

  private val closed: AtomicBoolean = new AtomicBoolean(false)

  def run(): Unit = {
    while (!closed.get()) {
      log.info("start consume")
      consume()
    }
  }

  def shutdown(): Unit = {
    log.info(s"shutdown consumer thread")
    closed.set(true)
    c.wakeup()
  }

  private def consume(): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(
      MonitoredThreadPoolExecutor
        .newFixedThreadPool(120, "lightcone-kafka-consumer")
    )
    c.subscribe(topics)
    try {
      while (true) {

        val records = c.poll(JDuration.ofMillis(5 * 1000L))

        breakable {
          records.forEach(r => {
            log.debug(
              s"handle offset ${r.offset()}, ${r.topic()}"
            )
            log.debug(
              s"handle record value ${r.value()}"
            )
            try {
              val handleResultFuture = handler.handle(r)
              if (syncConfig.isSync) {
                log.debug(
                  s"sync handle current offset is ${r.offset()}"
                )
                if (!Await.result(
                      handleResultFuture,
                      syncConfig.timeout.getOrElse(1 seconds)
                    )) {
                  c.seek(
                    new TopicPartition(r.topic(), r.partition()),
                    r.offset()
                  )
                  break
                } else {
                  commitByOffset(r)
                }
              } else {
                handleResultFuture.map { rst =>
                  log.info(
                    s"handle callback result is $rst, ${r
                      .topic()}, ${r.offset()}"
                  )
                }.recover {
                  case e: Exception => log.info(s"handle result exception: $e")
                }
              }
            } catch {
              case e: CommitFailedException =>
                log.info(
                  s"consumer unsubscribe because of commit failed, record:$r"
                )
                c.unsubscribe()
                throw e
              case e: Exception =>
                log.error(
                  s"handle callback exception , record:$r, error:${e.getMessage}",
                  e
                )
                if (syncConfig.isSync) {
                  c.seek(
                    new TopicPartition(r.topic(), r.partition()),
                    r.offset()
                  )
                  break
                }
            }
          })
        }
        if (!syncConfig.isSync) {
          log.debug("async commit offset")
          c.commitAsync()
        }
        log.debug(
          "end poll and handle"
        )
      }
    } catch {
      case e: Exception =>
        log.info(e.getMessage, e)
    } finally {
      log.info("consumer will unsubscribe")
      c.unsubscribe()
    }
  }

  private def commitByOffset(r: ConsumerRecord[String, T]): Unit = {

    log.debug(
      s"sync commit offset ${r.offset() + 1}"
    )
    val commitMap: java.util.Map[TopicPartition, OffsetAndMetadata] =
      new util.HashMap[TopicPartition, OffsetAndMetadata]()
    commitMap.put(
      new TopicPartition(r.topic(), r.partition()),
      new OffsetAndMetadata(r.offset() + 1)
    )
    c.commitSync(commitMap)
  }
}

case class SyncConfig(
    isSync: Boolean,
    timeout: Option[Duration])
