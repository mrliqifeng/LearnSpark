import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession



case class Person(name:String,age:Int){

}

object SqlTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session:SparkSession = SparkSession.builder.appName("name").master("local[2]").getOrCreate()
    val sql = session.sqlContext
    val js = sql.read.json("D:/Learn/douban.json").toDF()
    val csv = sql.read
      .option("header","true")
      .csv("D:/Learn/a_douban.csv")
//    js.select("name").show()
//    js.printSchema()
    js.createOrReplaceTempView("js")
    session.sql("select name from js")
    val sc:SparkContext = session.sparkContext
    case class Person(name: String, age: Long)
    import sql.implicits._
    val caseClassDS = sc.parallelize(List(1,2,3,4)).map(x=>(x,1)).toDF()
    caseClassDS.createOrReplaceTempView("df")
    sql.sql("select _2 from df").show()

  }

}
