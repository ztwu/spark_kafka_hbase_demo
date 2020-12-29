package org.apache.spark.sql.structured.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  */
class MySQLSource(sqlContext: SQLContext,
                  options: Map[String, String],
                  schemaOption: Option[StructType]) extends Source with Logging {

  override def schema: StructType = schemaOption.get

  /**
    * 获取Offset
    * 这里监控MySQL数据库表中条数变化情况
    * @return Option[Offset]
    */
  override def getOffset: Option[Offset] = {
    return null
  }

  /**
    * 获取数据
    * @param start 上一次的offset
    * @param end 最新的offset
    * @return df
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    return null
  }

  override def stop(): Unit = {
    return null
  }

}