/**
  * Copyright 2019 Loopring Foundation
  * Author: wxl@loopring.org (Wu XiaoLu)
  */

package io.lightcone.utils

import java.net.InetAddress

import com.typesafe.config.{Config, ConfigValueFactory}

import scala.util.Random

package object mq {

  private val envKey = "SCALA_ENV"
  private val r: Random = new Random()

  def confirmIPByEnv(conf: Config): Config = {
    if (System.getenv(envKey) == "test") {
      val host = InetAddress.getLocalHost
      val address: String = host.getHostAddress
      conf.withValue(
        "bootstrap.servers",
        ConfigValueFactory.fromAnyRef(s"$address:9092")
      )
    } else {
      conf
    }
  }

  def getTimeMillis: Long = System.currentTimeMillis()
  def getTimeSeconds: Long = getTimeMillis / 1000

  def randomStr: String = "%016d".format(math.abs(r.nextInt()))
}
