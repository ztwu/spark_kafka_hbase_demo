package com.iflytek.scala

import scala.collection.mutable.HashMap

object scalademo {
  def main(args: Array[String]): Unit = {
    val data = HashMap[String,Int]("a"->1,"b"->2)
    data("c") = 3
    println(data)
    for((key,value)<-data){
      println((key,value))
    }

    val data1 = Set(1,2,3,4,"a","b")
    for(i <- data1){
      println(i)
    }

    val data2 = List(1,2,6,3,4)
    for(i <- data2){
      println(i)
    }

//    var d:Option[(String,Int)] = Option("a",1)
    var d:Option[(String,Int)] = None
    println(d.getOrElse("a",2))

    data2.reverse.foreach(x=>println(x))
    data2.sorted.foreach(x=>println(x))

    var data44 = Tuple2(1,"ztwu4")
    data44.productIterator.foreach(x=>println(x))

  }

}
