package com.madhouse.ssp.util

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import com.madhouse.ssp.logger
import com.madhouse.ssp.entity.LogType._
import com.madhouse.ssp.entity._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.collection.mutable.Map

/**
  * Created by Sunxiang on 2017-08-04 17:38.
  *
  */
class JDBCSink(schema: StructType, logType: LogType, conf: JDBCConf) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"

  var connection: Connection = _
  var statement: Statement = _
  var reportData: Map[String, ReportData] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(conf.url, conf.user, conf.pwd)
    statement = connection.createStatement()
    reportData = Map[String, ReportData]()
    true
  }

  override def process(r: Row): Unit = {
    processData(r)
  }

  override def close(errorOrNull: Throwable): Unit = {
    saveData()
    connection.close()
  }

  def processData(r: Row) = {
    logType match {
      case MEDIABID =>
        val key = s"""${r.getInt(0)}, ${r.getInt(1)}, '${r.getString(2)}', ${r.getInt(3)}"""
        if (reportData.contains(key)) {
          val data = reportData.get(key).get
          data.reqs += r.getLong(4)
          data.bids += r.getLong(5)
          data.errs += r.getLong(6)
        } else reportData += (key -> ReportData(r.getLong(4), r.getLong(5), 0L, 0L, r.getLong(6), 0L, 0L, 0L, 0L, 0L, 0L))
      case DSPBID =>
        val key = s"""${r.getInt(0)}, ${r.getInt(1)}, '${r.getString(2)}', ${r.getInt(3)}"""
        if (reportData.contains(key)) {
          val data = reportData.get(key).get
          data.reqs += r.getLong(4)
          data.bids += r.getLong(5)
          data.wins +=  r.getLong(6)
          data.timeouts += r.getLong(7)
          data.errs +=  r.getLong(8)
        } else reportData += (key -> ReportData(r.getLong(4), r.getLong(5), r.getLong(6), r.getLong(7), r.getLong(8), 0L, 0L, 0L, 0L, 0L, 0L))
      case IMPRESSION | CLICK =>
        val key = s"""${r.getInt(0)}, ${r.getInt(1)}, ${r.getInt(2)}, ${r.getInt(3)}, '${r.getString(4)}', ${r.getInt(5)}"""
        if (reportData.contains(key)) {
          val data = reportData.get(key).get
          data.imps += r.getLong(6)
          data.clks += r.getLong(7)
          data.vimps += r.getLong(8)
          data.vclks += r.getLong(9)
          data.income +=  r.getLong(10)
          data.cost += r.getLong(11)
        } else reportData += (key -> ReportData(0L, 0L, 0L, 0L, 0L, r.getLong(6), r.getLong(7), r.getLong(8), r.getLong(9), r.getLong(10), r.getLong(11)))
    }

    if (reportData.size % conf.batchSize == 0) saveData()
  }

  def saveData() = {
    val fields = schema.fields map { _.name }

    val values = reportData map { case (k, v) =>
      s"($k, ${v.toSeq(logType).mkString(", ")})"
    }

    if (values.nonEmpty) {
      val sql = s"""INSERT INTO `${conf.table}` (${fields.mkString("`", "`, `", "`")}) VALUES ${values.mkString(", ")}"""
      logger(sql)
      statement.execute(sql)
    }

    reportData.clear()
  }
}