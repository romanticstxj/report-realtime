package com.madhouse.ssp.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Sunxiang on 2017-08-04 17:38.
  *
  */
class JDBCSink(schema: StructType, conf: JDBCConf) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: PreparedStatement = _

  private var size = 0

  val preSql = {
    val fields = schema map { f => f.name }
    val placeholders = 0 until fields.size map { _ => "?" }

    s"INSERT INTO `${conf.table}` (${fields.mkString("`", "`, `", "`")}) VALUES (${placeholders.mkString(", ")})"
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(conf.url, conf.user, conf.pwd)
    statement = connection.prepareStatement(preSql)
    true
  }

  override def process(r: Row): Unit = {
    schema.fields.zipWithIndex foreach { e =>
      val idx = e._2 + 1
      e._1.dataType match {
        case IntegerType => statement.setInt(idx, r.getInt(e._2))
        case LongType => statement.setLong(idx, r.getLong(e._2))
        case _ => statement.setString(idx, r.getString(e._2))
      }
    }
    statement.addBatch()
    size += 1

    if (size % conf.batchSize == 0) statement.executeBatch()
  }

  override def close(errorOrNull: Throwable): Unit = {
    statement.executeBatch()
    connection.close()
  }
}
