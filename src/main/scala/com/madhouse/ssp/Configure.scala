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

  val fs: FileSystem = sys.props.get("hadoop.env") match {
    case Some(env) =>
      logger(s"hadoop environment: $env")
      FileSystem.get {
        val conf = ConfigFactory.load("hadoop").getConfig(env)

        val configuration = new Configuration()
        conf.entrySet().iterator() foreach { c =>
          val key = c.getKey
          configuration.set(c.getKey, conf.getString(key))
        }
        configuration
      }
    case None =>
      throw new IllegalArgumentException("hadoop.env is not set")
  }

  implicit private val config: Config = sys.props.get("config.file") match {
    case Some(file) =>
      logger(s"config file: $file")
      if (file.startsWith("file://")) {
        ConfigFactory.parseFile(new File(new URI(file))).getConfig("app")
      } else {
        val path = new Path(file)
        ConfigFactory.parseReader(new InputStreamReader(fs.open(path, 10240))).getConfig("app")
      }
    case None =>
      throw new IllegalArgumentException("config.file is not set")
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

  val sparkMaster = getOrElse("spark.master", "local[*]")
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
    val table = getOrElse("dest_table_name", "mad_report_media_rt_mem")(conf)
    val size = getOrElse("batch_size", 64)
    JDBCConf(url, user, pwd, table, size)
  }

  val logType = LogType.withName(getOrElse("log_type", ""))
}
