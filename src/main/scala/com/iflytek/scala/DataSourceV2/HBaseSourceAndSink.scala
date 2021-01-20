package com.iflytek.scala.DataSourceV2

import java.util
import java.util.Optional

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * @author cherish
  * @create 2020-04-26 10:49
  */
object HBaseSourceAndSink {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("sparkSqlSourceAndSink")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //format 需要我们自定义数据源
    val df: DataFrame = spark.read.format("com.travel.programApp.HBaseSource")
      .option("hbase.table.name", "spark_hbase_sql") //我们自己带的一些参数
      .option("cf.cc", "cf:name,cf:score") //定义我们查询hbase的那些列
      .option("schema", "`name` STRING , `score` STRING") //定义我们表的schema 返回的数据是按照循序定义的
      .load

    df.createOrReplaceTempView("sparkHBaseSQL")

    df.printSchema()

    //分析得到的结果数据 , 将结果数据保存到hbase , redis 或者 mysql 中 或者es
    val resultDF: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 70 ")

    resultDF.write.format("com.travel.programApp.HBaseSource")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name" , "spark_hbase_write")
      .option("cf" , "cf")
      .save()


  }
}

//自定义数据源,实现数据的查询
class  HBaseSource extends DataSourceV2 with ReadSupport with WriteSupport{
  /**
    * 定义我们映射的表的schema
    * @param options
    * @return
    */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    //从spark.read.format().option() 里面传过来的
    val tableName  :String = options.get("hbase.table.name").get()
    val cfAndCC :String = options.get("cf.cc").get()
    val schema:String =  options.get("schema").get()


    new HBaseDataSourceReader(tableName , cfAndCC , schema)
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    //从spark.read.format().option() 里面传过来的
    val tableName  :String = options.get("hbase.table.name").get()
    val family :String = options.get("cf").get()


    Optional.of(new HBaseDataSourceWriter(tableName))
  }
}

class HBaseDataSourceWriter(tableName : String) extends DataSourceWriter {

  /**
    * 将数据保存起来全部依赖这个方法
    * @return
    */
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HBaseDataWriterFactory(tableName)
  }

  //数据提方法
  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  //放弃数据的插入方法
  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

class HBaseDataWriterFactory(tableName : String) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HBaseDataWriter(tableName)
  }
}

class HBaseDataWriter(tableName :String) extends DataWriter[Row] {

  private val conn = HbaseTools.getHbaseConn
  private val table: Table = conn.getTable(TableName.valueOf(tableName))

  //写入数据
  override def write(record: Row): Unit = {
    val name:String = record.getString(0)
    val score  :  String = record.getString(1)

    val put = new Put("0001".getBytes())

    put.addColumn("cf".getBytes() , "name".getBytes() ,  name.getBytes())
    put.addColumn("cf".getBytes() , "score".getBytes() , score.getBytes())

    table.put(put)

  }

  //数据的提价方法,数据插入完成之后,在这个方法里面进行数据的事务提交
  override def commit(): WriterCommitMessage = {
    //hbase 里面没有事务 , 所以在这里就把 table 和 conn 关闭 , 然后返回null
    table.close()
    conn.close()
    null
  }

  override def abort(): Unit = {

  }
}

class HBaseDataSourceReader(tableName:String , cfAndCC:String , schema:String) extends DataSourceReader {
  override def readSchema(): StructType = {
    StructType.fromDDL(schema)
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    /**
      * new HBaseDateReaderFactory().asInstanceOf[DataReaderFactory[Row] ]:
      * 将 HBaseDateReaderFactory() 转成 DataReaderFactory[Row]  对象
      * Seq[] scala集合
      * as.Java  将 scala seq 集合转成 java 集合
      */
    import scala.collection.JavaConverters._
    Seq(new HBaseDateReaderFactory(tableName , cfAndCC).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}
class HBaseDateReaderFactory(tableName:String,cfAndCC:String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    new HBaseDataReader(tableName , cfAndCC);
  }
}

/**
  * 自定义HBaseDateReader 实现dateReader接口
  *
  */
class HBaseDataReader(tableName :String , cfAndCC:String) extends DataReader[Row] {

  var conn: Connection = null
  var table: Table = null

  var scan = new Scan()
  var resultScanner: ResultScanner = null

  /**
    * 使用ProtobufUtil将sparkContext对象序列化成为一个字符串传下来,下面在反序列化
    */

  /**
    * 获取我们hbase的数据就在这
    * @return
    */
  def getIterator: Iterator[Seq[AnyRef]] = {
    conn = HbaseTools.getHbaseConn
    table = conn.getTable(TableName.valueOf(tableName))
    resultScanner = table.getScanner(scan)

    import scala.collection.JavaConverters._

    val iterator: Iterator[Seq[AnyRef]] = resultScanner.iterator().asScala.map(eachResult => {

      /*val cfCCArr: Array[String] = cfAndCC.split(",")
      val family: String = cfCCArr(0).split(":")(0)

      val clumn1: String = cfCCArr(0).split(":")(1)
      val clumn2: String = cfCCArr(2).split(":")(1)*/

      val name: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "name".getBytes()))
      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "score".getBytes()))
      System.out.println("===================================")
      System.out.println(Seq(name, score).toString())
      System.out.println("===================================")

      Seq(name, score)
    })
    iterator

  }

  val data:Iterator[Seq[AnyRef]] = getIterator

  /**
    * 这个方法反复不断的被调用,只要我们查询到了数据,就可以使用next方法一直获取下一条数据
    * @return
    */
  override def next(): Boolean = {
    data.hasNext
  }



  /**
    * 获取到的数据在这个方法里面一条条的解析,解析之后,映射到我们提前定义的表里面去
    * @return
    */
  override def get(): Row ={
    Row.fromSeq(data.next())
  }

  /**
    *
    */
  override def close(): Unit = {
    table.close()
    conn.close()
  }
}
