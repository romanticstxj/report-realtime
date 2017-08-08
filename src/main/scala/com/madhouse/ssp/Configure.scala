package com.madhouse.ssp

import java.io.{File, InputStreamReader}
import java.net.URI

import com.madhouse.ssp.entity.LogType
import com.madhouse.ssp.util.JDBCConf
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConversions._

/**
  * Created by Sunxiang on 2017-07-27 09:12.
  */
object Configure {

  var fs: FileSystem = _

  implicit var config: Config = _

  def initConf(hadoopEnv: String, conf: String) = {
    println(s"hadoop env: $hadoopEnv, config path: $conf")

    fs = FileSystem.get {
      val conf = ConfigFactory.load("hadoop").getConfig(hadoopEnv)

      val configuration = new Configuration()
      conf.entrySet().iterator() foreach { c =>
        val key = c.getKey
        configuration.set(c.getKey, conf.getString(key))
      }
      configuration
    }

    config = if (conf.startsWith("file://")) {
      ConfigFactory.parseFile(new File(new URI(conf))).getConfig("app")
    } else {
      val path = new Path(conf)
      ConfigFactory.parseReader(new InputStreamReader(fs.open(path, 10240))).getConfig("app")
    }
  }


  private def getOrElse[T](path: String, default: T)(implicit config: Config) = {
    if (config.hasPath(path))
      default match {
        case _: String => config.getString(path).asInstanceOf[T]
        case _: Int => config.getInt(path).asInstanceOf[T]
        case _: Long => config.getLong(path).asInstanceOf[T]
        case _: Boolean => config.getBoolean(path).asInstanceOf[T]
        case _ => default
      }
    else default
  }

  lazy val sparkMaster = getOrElse("spark.master", "local[*]")
  lazy val startingOffsets = getOrElse("spark.streaming.starting_offsets", "earliest")
  lazy val maxOffsetsPerTrigger = getOrElse("spark.streaming.max_offsets_per_trigger", "10240")
  lazy val processingTimeMs = getOrElse("spark.streaming.trigger_processing_time_ms", 30000)

  lazy val kafkaBootstrapServers = getOrElse("kafka.bootstrap_servers", "localhost:9092")
  lazy val topicName = getOrElse("kafka.topic_name", "")

  lazy val jdbcConf = {
    val conf = config.getConfig("mysql")
    val url = conf.getString("url")
    val user = getOrElse("user", "root")(conf)
    val pwd = getOrElse("pwd", "123456")(conf)
    val table = getOrElse("dest_table_name", "mad_report_media_rt_mem")(conf)
    val size = getOrElse("batch_size", 64)
    JDBCConf(url, user, pwd, table, size)
  }

  lazy val logType = LogType.withName(getOrElse("log_type", ""))
}
