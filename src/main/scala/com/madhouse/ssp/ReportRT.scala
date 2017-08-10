package com.madhouse.ssp

import com.madhouse.ssp.avro.{ClickTrack, DSPBid, ImpressionTrack, MediaBid}
import com.madhouse.ssp.entity._
import com.madhouse.ssp.util.{AvroUtil, JDBCSink}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object ReportRT {
  import Configure._
  import LogType._

  def main(args: Array[String]): Unit = {
     val (hadoopEnv, configFile) = args.length match {
       case 1 => ("production", args(0))
       case 2 => (args(0), args(1))
       case _ => throw new IllegalArgumentException("the parameter length must equal 1 or 2")
     }

    require(Seq("develop", "beta", "production").contains(hadoopEnv), s"invalid hadoop environment $hadoopEnv")

    initConf(hadoopEnv, configFile)

    val spark = SparkSession.builder.appName(s"ReportRT-$logType").master(sparkMaster).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.addResource(fs.getConf)

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> kafkaBootstrapServers,
        "subscribe" -> topicName,
        "startingOffsets" -> startingOffsets,
        "maxOffsetsPerTrigger" -> maxOffsetsPerTrigger
      ))
      .load()
      .selectExpr("value")
      .as[Array[Byte]]

    val result = logType match {
      case MEDIABID =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[MediaBid](bytes)
          MediaBidRecord(r.getRequest.getMediaid, r.getRequest.getAdspaceid, dayHour(r.getTime), mediaCount(r.getStatus))
        }
        rs.groupBy('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'day, 'hour).agg(sum('reqs) as 'reqs, sum('bids) as 'bids, sum('errs) as 'errs)
      case DSPBID =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[DSPBid](bytes)
          DspBidRecord(r.getPolicyid, r.getDspid, dayHour(r.getTime), dspCount(r.getStatus, r.getWinner))
        }
        rs.groupBy('policyId as 'policy_id, 'dspId as 'dsp_id, 'day, 'hour).agg(sum('reqs) as 'reqs, sum('bids) as 'bids, sum('wins) as 'wins, sum('timeouts) as 'timeouts, sum('errs) as 'errs)
      case IMPRESSION =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[ImpressionTrack](bytes)
          ImpressionRecord(r.getMediaid, r.getAdspaceid, r.getPolicyid, r.getDspid, dayHour(r.getTime), impClkCount(r.getInvalid), r.getIncome, r.getCost)
        }
        rs.groupBy('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'policyId as 'policy_id, 'dspId as 'dsp_id, 'day, 'hour).agg(sum('imps) as 'imps, sum('vimps) as 'vimps, sum('income) as 'income, sum('cost) as 'cost)
      case CLICK =>
        val rs = ds map { bytes =>
          val r = AvroUtil.recordDecode[ClickTrack](bytes)
          ClickRecord(r.getMediaid, r.getAdspaceid, r.getPolicyid, r.getDspid, dayHour(r.getTime), impClkCount(r.getStatus), r.getIncome, r.getCost)
        }
        rs.groupBy('mediaId as 'media_id, 'adSpaceId as 'adspace_id, 'policyId as 'policy_id, 'dspId as 'dsp_id, 'day, 'hour).agg(sum('clks) as 'clks, sum('vclks) as 'vclks, sum('income) as 'income, sum('cost) as 'cost)
    }

    val schema = result.schema

    result.writeStream
      .outputMode("append")
      .foreach(new JDBCSink(schema, jdbcConf))
      .option("checkpointLocation", s"/spark/checkpoint/$topicName")
      .trigger(Trigger.ProcessingTime(processingTimeMs millis))
      .start()
      .awaitTermination()
  }

}
