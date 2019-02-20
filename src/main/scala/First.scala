import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
object First {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setMaster("local").setAppName("First")
    val sc:SparkContext = SparkContext.getOrCreate(sparkConf)
//    createPair(sc)
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

  def createPair(sc:SparkContext):Unit={
    val pairs = sc.parallelize(Array("Hello world","Hello liqifeng","Nihao wuyachen"))
    val pair2 = pairs.map(line => (line.split(" ")(0),line))
    pair2.foreach(println)
  }
}
