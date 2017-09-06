package com.madhouse.ssp

import java.io.{File, InputStreamReader}
import java.net.URI

import com.madhouse.ssp.entity.LogType
import com.madhouse.ssp.util.JDBCConf
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by Sunxiang on 2017-07-27 09:12.
  */
class Configure(file: String) extends Serializable {

  val fs: FileSystem = FileSystem.get(new Configuration())

  implicit private val config: Config = {
    logger(s"config file: $file")
    val uri = new URI(file)
    uri.getScheme match {
      case "file" => ConfigFactory.parseFile(new File(uri)).getConfig("app")
      case "hdfs" => ConfigFactory.parseReader(new InputStreamReader(fs.open(new Path(uri), 10240))).getConfig("app")
      case _ => throw new IllegalArgumentException(s"unknown config: $file")
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

  val startingOffsets = getOrElse("spark.streaming.starting_offsets", "latest")
  val maxOffsetsPerTrigger = getOrElse("spark.streaming.max_offsets_per_trigger", "10240")
  val processingTimeMs = getOrElse("spark.streaming.trigger_processing_time_ms", 30000)

  val kafkaBootstrapServers = getOrElse("kafka.bootstrap_servers", "localhost:9092")
  val topicName = getOrElse("kafka.topic_name", "")

  val jdbcConf = {
    val conf = config.getConfig("mysql")
    val url = conf.getString("url")
    val user = getOrElse("user", "root")(conf)
    val pwd = getOrElse("pwd", "123456")(conf)
    val table = conf.getString("dest_table_name")
    val size = getOrElse("batch_size", 64)
    JDBCConf(url, user, pwd, table, size)
  }

  val logType = LogType.withName(getOrElse("log_type", ""))
}
