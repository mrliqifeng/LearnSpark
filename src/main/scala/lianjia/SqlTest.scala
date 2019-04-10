package lianjia

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SqlTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session:SparkSession = SparkSession.builder.appName("name").master("local[2]").getOrCreate()
    //构造url链接
    var jdbc_url:String = "jdbc:mysql://localhost:3306/scrapy?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    var jdbc_map:Map[String,String] = Map()
    val props = new Properties
    props.put("user","root")
    props.put("password","000000")
    val lianjia: DataFrame = session.read.jdbc(jdbc_url,"a_链家",props)
    lianjia.printSchema()
    lianjia.limit(10).show()

  }
}