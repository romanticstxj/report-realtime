package com.madhouse.ssp.entity


/**
  * Created by Sunxiang on 2017-08-02 15:03.
  *
  */
trait Record extends Serializable

case class MediaBidRecord(mediaId: Int, adSpaceId: Int, day: String, hour: Int, reqs: Long, bids: Long, errs: Long) extends Record

case class DspBidRecord(policyId: Int, dspId: Int, day: String, hour: Int, reqs: Long, bids: Long, wins: Long, timeouts: Long, errs: Long) extends Record

case class ImpressionRecord(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, day: String, hour: Int, imps: Long, vimps: Long, income: Long, cost: Long) extends Record

case class ClickRecord(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, day: String, hour: Int, imps: Long, vimps: Long, income: Long, cost: Long) extends Record

object MediaBidRecord {
  def apply(mediaId: Int, adSpaceId: Int, dayHour: String, count: (Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }
    new MediaBidRecord(mediaId, adSpaceId, day, hour, count._1, count._2, count._3)
  }
}

object DspBidRecord {
  def apply(policyId: Int, dspId: Int, dayHour: String, count: (Long, Long, Long, Long, Long)) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }
    new DspBidRecord(policyId, dspId, day, hour, count._1, count._2, count._3, count._4, count._5)
  }
}

private [ssp] object ImpressionRecord {
  def apply(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, dayHour: String, count: (Long, Long), income: Long, cost: Long) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }

    new ImpressionRecord(mediaId, adSpaceId, policyId, dspId, day, hour, count._1, count._2, income * 10, cost * 10)
  }
}

private [ssp]object ClickRecord {
  def apply(mediaId: Int, adSpaceId: Int, policyId: Int, dspId: Int, dayHour: String, count: (Long, Long), income: Long, cost: Long) = {
    val (day, hour) = {
      val ps = dayHour.split('_')
      (ps(0), (ps(1).toInt))
    }

    new ClickRecord(mediaId, adSpaceId, policyId, dspId, day, hour, count._1, count._2, income * 10000, cost * 101)
  }
}