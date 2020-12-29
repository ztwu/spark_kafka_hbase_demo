package iflytek.scala

import com.iflytek.iflytek

/**
 * Hello world!
 *
 */
object Main extends App {
  val test = new iflytek()
  test.test()
  println( "Hello World!" )

  val data = Tuple4(1,2,3,4)
  println(data)

  val data2 = Array(1,2,3,4)
  println(data2.getClass)

  data2.foreach((i:Int) => {
    val r = i+1;
    println(i,r)
  })

  data.productIterator.foreach(i => {
    val r = i.toString.toInt+2;
    println(i,r)
  })

  val d = "你好啊你"
  val data3 = d.map(x=>{
    println(x)
    (x,1)
  }).groupBy(_._1).map(x=>(x._1, x._2))
  println(data3)

  val data4 = data3.mapValues(x=>{
    println(x)
    x.map(x=>{x._2})
  }).map(x=>x._2.sum)
  println(data4)

  var words = Set("hive", "hbase", "redis")
  val result = words.flatMap(x => x.toUpperCase)
  println(result)

  var words2 = scala.collection.mutable.Set("hive", "hbase", "redis")
  words2.add("test")
  println(words2)

  words2 = scala.collection.mutable.Set("hive2", "hbase", "redis")

  val a = "abv"
  a match {
    case "a" => println("a")
    case "b" => println("b")
    case "abv" => println("c")
  }

  println(a.matches("\\w+"))

  class User(var name:String, val id:Int, text:String){

    val u_id = id
    val u_name = name

    def test(b:String): Unit ={
      name = name + "123121212"
      println("hello",b,name)
    }
  }

  val user = new User("ztwu4",1,"test")
  user.test("123")
  println(user.name)

  val aa:Option[Int] = Some(5)
  val ba:Option[Int] = None

  println("a.getOrElse(0): " + aa.getOrElse(0) )
  println("b.getOrElse(10): " + ba.getOrElse(10) )

  val sites = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")

  println("show(sites.get( \"runoob\")) : " +
    show(sites.get( "runoob")) )
  println("show(sites.get( \"baidu\")) : " +
    show(sites.get( "baidu")) )
  println(sites.get( "baidu"))

  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }

  val lista = List(1,2,3,4)
  for(i <- lista){
    val tt:Option[Int] = Some(i)
    println(tt)
  }

  var aaa:Option[String] = Some("00")
  println(aaa.getOrElse())
  aaa = None
  println(aaa.getOrElse("12"))

  val data8 = List(List(1,2,3),List(1,5,2,4))
  val testdata = data8.flatten.map(x=>{
    println(x,1)
    (x,1)
  }).groupBy(x=>{
    println(x._1,x._2)
    x._1
  })
  println(testdata)
  val testdata2 = testdata.mapValues(x=>{
    println("x:",x)
    x.map(x=>x._2)
  })
  println(testdata2)
  testdata2.map(x=>(x._1,x._2.length)).map(x=>println(x))

  val data01 = List(1,2,3,4)
  val data02 = List("a","b","c","d")
  data02.zip(data01).foreach(x=>println(x))

  testdata.groupBy(_._1).foreach(x=>println(x))



}


