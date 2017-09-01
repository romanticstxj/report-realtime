package com.madhouse

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{Instant, LocalDateTime, ZoneId}

/**
  * Created by Sunxiang on 2017-07-27 09:10.
  */
package object ssp {
  val zone = ZoneId.of("Asia/Shanghai")

  val logger: String => Unit = { msg =>
    val time = LocalDateTime.ofInstant(Instant.now(), zone).format(ofPattern("yyyy-MM-dd HH:mm:ss"))
    println(s"[$time] $msg")
  }

  val dayHour = (ts: Long) => {
    val instant = Instant.ofEpochMilli(ts)
    val ldt = LocalDateTime.ofInstant(instant, zone)
    ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_H"))
  }

  val mediaCount = (status: Int) => {
    status match {     //(reqs, bids, errs)
      case 200        => (1L, 1L, 0L)
      case 400 | 500  => (1L, 0L, 1L)
      case _          => (1L, 0L, 0L)
    }
  }

  val dspCount = (status: Int, winner: Int) => {
    val wins = if (winner > 0) 1L else 0L
    status match {     //(reqs, bids, wins, tos, errs)
      case 200        => (1L, 1L, wins, 0L, 0L)
      case 400 | 500  => (1L, 0L, 0L, 0L, 1L)
      case 408        => (1L, 0L, 0L, 1L, 0L)
      case _          => (1L, 0L, 0L, 0L, 0L)
    }
  }

  val trackerCountAndMoney = (status: Int, income: Long, cost: Long) => {
    status match {
      case 0 => (1L, 1L, income, cost)
      case x if (x > 0) => (1L, 0L, 0L, 0L)
      case _ => (0L, 0L, 0L, 0L)
    }
  }
}
