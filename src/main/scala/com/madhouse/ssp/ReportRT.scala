package com.madhouse.ssp

import com.madhouse.ssp.avro.{ClickTrack, DSPBid, ImpressionTrack, MediaBid}
import com.madhouse.ssp.entity._
import com.madhouse.ssp.util.{AvroUtil, JDBCSink}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object ReportRT {
  import LogType._

  def main(args: Array[String]): Unit = {

    require(args.length == 1, s"config file is not set")

    implicit val configure = new Configure(args(0))
    import configure._

    val logType = configure.logType

    val spark = SparkSession.builder.appName(s"ReportRT-$logType").master("local[2]").getOrCreate()

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topicName)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select("value")
      .as[Array[Byte]]

    val result = logType match {
      case MEDIABID =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[MediaBid](bytes, logType)
          MediaBidRecord(r.getRequest.getMediaid.toInt, r.getRequest.getAdspaceid.toInt, dayHour(r.getTime), mediaCount(r.getStatus))
        }
        rs.select('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'date, 'hour, 'reqs, 'bids, 'errs)
      case DSPBID =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[DSPBid](bytes, logType)
          DspBidRecord(r.getPolicyid.toInt, r.getDspid.toInt, dayHour(r.getTime), dspCount(r.getStatus, r.getWinner))
        }
        rs.select('policyId as 'policy_id, 'dspId as 'dsp_id, 'date, 'hour, 'reqs, 'bids, 'wins, 'timeouts, 'errs)
      case IMPRESSION =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[ImpressionTrack](bytes, logType)
          ImpressionRecord(r.getMediaid.toInt, r.getAdspaceid.toInt, r.getPolicyid.toInt, r.getDspid.toInt, dayHour(r.getTime), trackerCountAndMoney(r.getInvalid, r.getIncome.toLong, r.getCost.toLong))
        }
        rs.select('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'policyId as 'policy_id, 'dspId as 'dsp_id, 'date, 'hour, 'imps, 'clks, 'vimps, 'vclks, 'income, 'cost)
      case CLICK =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[ClickTrack](bytes, logType)
          ClickRecord(r.getMediaid.toInt, r.getAdspaceid.toInt, r.getPolicyid.toInt, r.getDspid.toInt, dayHour(r.getTime), trackerCountAndMoney(r.getInvalid, r.getIncome.toLong, r.getCost.toLong))
        }
        rs.select('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'policyId as 'policy_id, 'dspId as 'dsp_id, 'date, 'hour, 'imps, 'clks, 'vimps, 'vclks, 'income, 'cost)
    }

    val schema = result.schema

    result.writeStream
      .queryName(s"ReportRT-$logType-Query")
      .outputMode("append")
      .foreach(new JDBCSink(schema, logType, jdbcConf))
      .option("checkpointLocation", s"/madssp/spark/checkpoint/$topicName")
      .trigger(Trigger.ProcessingTime(processingTimeMs millis))
      .start()
      .awaitTermination()
  }

}
