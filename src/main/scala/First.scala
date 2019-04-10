import java.io.StringReader
import java.sql.DriverManager
import java.util

import com.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
object First {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val sparkConf = new SparkConf().setMaster("local").setAppName("First")
//    val sc:SparkContext = SparkContext.getOrCreate(sparkConf)
//    csvTest(sc)
    var map:Map[String,String] = Map()
    insertMap(map)
  }
  def insertMap(map:Map[String,String]):Unit={
    var conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/scrapy?serverTimezone=GMT", "root", "000000")
    var ps = conn.prepareStatement("insert into users(username,pwd) values (?,?)")
    ps.setString(1,"liqifeng")
    ps.setString(2,"liqifeng")
    ps.execute()

  }

  def junFile(sc:SparkContext):Unit={
    val input = sc.wholeTextFiles("D:/Learn")
    val resu = input.map{case(x,y)=>
        val re = y.replace("\r\n"," ").split(" ").map(line=>line.toDouble)
      (x,re.sum/re.length.toDouble)
    }
    resu.foreach(println)
  }

  def csvTest(sc:SparkContext):Unit={
    val input = sc.textFile("D:\\QMDownload\\a_douban.csv")
    val result = input.map{line=>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    result.foreach{line=>
      line.foreach(lin=>print(lin+","))
      println()
    }
  }

  def reByKey(sc:SparkContext):Unit={
    val chu = sc.parallelize(List(("li",1),("qi",2),("li",2),("fen",2),("qi",2)))
    val jun = chu.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
//    jun.foreach(println)
    chu.collect().foreach(println)
  }

  def pairTest(sc:SparkContext):Unit={
    var map:Map[String,String] = Map()
    map += ("one"->"one")
    map += ("two"->"two")
    map += ("three"->"three")
    map += ("four"->"four")
    var array = map.toArray
    val arr = sc.parallelize(array)
    val li = sc.parallelize(List(1,1,2,3,3,4))
    val ll = li.map(line=>(line.toString,1))
    val ii = sc.parallelize(List((1,1),(2,1),(3,1),(2,1)))
    val ii2 = ii.map{case(x,y)=>(y.toString,1)}
    val joi = ll.join(ii2)
    joi.cogroup(ii2).foreach(println)
    val fil = ll.filter{case(_,y)=> y>1}
  }

  /**
    * 单次计数
    * @param sc spark驱动器
    */
  def count(sc:SparkContext):Unit={
    val firstRdd:RDD[String] = sc.textFile("D:/one.txt")
    val words = firstRdd.flatMap(line => line.split(" "))
    val counts = words.map(word=>(word,1)).reduceByKey{case(x,y)=>x+y}
    counts.foreach{line=>
      println(line)
    }
    var li:util.ArrayList[String] = new util.ArrayList[String]()
    val li2:List[String] = List("one","two")
  }

  def test(sc:SparkContext):Unit={
    val text = sc.textFile("D:/one.txt")
    val error = text.filter{line=>
      line.contains("a")
    }
    val one:RDD[String] = sc.parallelize(Array("one","two","three"))
    val two:RDD[String] = sc.parallelize(List("four","five"))
    val three = one.union(two)
    three.persist()
    three.foreach(line=>println(line))
    println(three.count())
    three.take(2).foreach(line=> println(line))
    var threeArray:Array[String] = three.collect()
    println("接下来是collect函数")
    threeArray.foreach{line=>
      println(line)
    }
  }

  def te2(sc:SparkContext):Unit={
    val data = sc.parallelize(List(2,2,4,1,4,21,343,2,4,3,4,2,3))
    val data2 = data.aggregate((0,0))(
      (acc,number)=> (acc._1+number,acc._2+1),
      (par1, par2) => (par1._1+par2._1, par1._2+par2._2)
    )
    println(data2)
  }
}
