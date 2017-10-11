package com.madhouse.ssp.entity

import com.madhouse.ssp.entity.LogType._


/**
  * Created by Sunxiang on 2017-08-02 15:03.
  *
  */
trait Record extends Serializable

case class MediaBidRecord(mediaId: Int, adSpaceId: Int, date: String, hour: Int, reqs: Long, bids: Long, errs: Long) extends Record

case class DspBidRecord(policyId: Int, dspId: Int, date: String, hour: Int, reqs: Long, bids: Long, wins: Long, timeouts: Long, errs: Long) extends Record

case class TrackerRecord(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, date: String, hour: Int, imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long) extends Record

case class ReportData(var reqs: Long = 0L, var bids: Long = 0L, var wins: Long = 0L, var timeouts: Long = 0L, var errs: Long = 0L, var imps: Long = 0L, var clks: Long = 0L, var vimps: Long = 0L, var vclks: Long = 0L, var income: Long = 0L, var cost: Long = 0L) {

  def toSeq(logType: LogType) = logType match {
    case MEDIABID => Seq(reqs, bids, errs)
    case DSPBID => Seq(reqs, bids, wins, timeouts, errs)
    case IMPRESSION | CLICK => Seq(imps, clks, vimps, vclks, income, cost)
  }
}


case object MediaBidRecord {
  def apply(mediaId: Int, adSpaceId: Int, dayHour: String, count: (Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }
    new MediaBidRecord(mediaId, adSpaceId, day, hour, count._1, count._2, count._3)
  }
}

case object DspBidRecord {
  def apply(policyId: Int, dspId: Int, dayHour: String, count: (Long, Long, Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }
    new DspBidRecord(policyId, dspId, day, hour, count._1, count._2, count._3, count._4, count._5)
  }
}

case object ImpressionRecord {
  def apply(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, dayHour: String, countAndMoney: (Long, Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }

    new TrackerRecord(mediaId, adSpaceId, policyId, dspId, day, hour, countAndMoney._1, 0L, countAndMoney._2, 0L, countAndMoney._3, countAndMoney._4)
  }
}

case object ClickRecord {
  def apply(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, dayHour: String, countAndMoney: (Long, Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }

    new TrackerRecord(mediaId, adSpaceId, policyId, dspId, day, hour, 0L, countAndMoney._1, 0L, countAndMoney._2, countAndMoney._3 * 1000, countAndMoney._4 * 1000)
  }
}