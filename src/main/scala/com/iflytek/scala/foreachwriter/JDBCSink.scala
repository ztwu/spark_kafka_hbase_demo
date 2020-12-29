package com.iflytek.scala.foreachwriter

import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSink(url: String, userName: String, password: String) extends ForeachWriter[Row]{
  val driver = "com.mysql.jdbc.Driver"
  var connection:Connection = _
  var statement:Statement = _

  override def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, userName, password)
    statement = connection.createStatement
    true
  }

  override def process(value: Row): Unit = {
    val id = value.getAs[String](0)
    val count = value.getAs[Long](1)
    statement.executeUpdate("INSERT INTO zip_test " +
      s"VALUES(${id}, ${count})")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
