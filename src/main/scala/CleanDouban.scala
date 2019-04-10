import com.alibaba.fastjson.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CleanDouban {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("CleanDouban").master("local[2]").getOrCreate()
    val sc = session.sqlContext
    val douban_csv = sc.read.csv("douban.csv").toJavaRDD.rdd
    val douban = douban_csv.map{line=>
      var json = new JSONObject()
      json.put("type",line(0))
      json.put("info",line(1))
      json.put("title",line(2))
      json.put("star",line(3))
      json.put("people",line(4))
      json.put("ebook","no")
      json
    }
    val douban_error = douban.filter{json=>
      var infoString = json.getString("info")
      val infos:Array[String] = infoString.split("/")
      var infoMap:Map[String,String] = Map()
      if(infos.length>=3){
        var error = false
        try{
          var name = infos(0)
          val date = infos(infos.length-2).trim
          val moneyString = infos(infos.length-1).trim
          infoMap += ("monney" -> moneyString)
          infoMap += ("year" -> date.split("-")(0))
          infoMap += ("date" -> date)
          infoMap += ("name" -> name)
          infoMap += ("type" -> json.getString("type").split(": ")(1).trim)
          infoMap += ("people" -> json.getString("people").split('(')(1).split("人")(0).trim)
          infoMap += ("ebook" -> json.getString("ebook"))
          infoMap += ("star" -> json.getString("star"))
          infoMap += ("title" -> json.getString("title"))
          error = true
        } catch{
          case e:Exception=>
            error
        }
        error
      }
      else{
        false
      }
    }
    println(douban_error.collect().length)
    println(douban.collect.length)
  }
}
