import java.sql.DriverManager

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
//    val session = SparkSession.builder().master("local[2]").appName("one").getOrCreate()
//    val lines = ssc.socketTextStream("localhost",7777)
//    val errorLine = lines.filter(_.contains("error"))
//    errorLine.print()
//    ssc.start()
//    ssc.awaitTermination()


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("spark")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val douban = stream.map{
      record =>
        val json = JSON.parseObject(record.value())
        var infoString = json.getString("info")
        val infos: Array[String] = infoString.split("/")
        var infoMap: Map[String, String] = Map()
        if (infos.length >= 3) {
          try {
            var name = infos(0)
            val date = infos(infos.length - 2).trim
            val moneyString = infos(infos.length - 1).trim
            infoMap += ("monney" -> moneyString)
            infoMap += ("year" -> date.split("-")(0))
            infoMap += ("date" -> date)
            infoMap += ("name" -> name)
            infoMap += ("type" -> json.getString("type").split(": ")(1).trim)
            infoMap += ("people" -> json.getString("people").split('(')(1).split("人")(0).trim)
            infoMap += ("ebook" -> json.getString("ebook"))
            infoMap += ("star" -> json.getString("star"))
            infoMap += ("title" -> json.getString("title"))
          } catch {
            case _: Exception =>infoMap=infoMap.empty
          }
          infoMap
        } else{
          infoMap
        }
    }
    val douban_clean = douban.filter{map=>
      if(map.nonEmpty){
        true
      } else{
        false
      }
    }
    douban_clean.foreachRDD(rdd=>rdd.foreach(println))
    ssc.start()
    ssc.awaitTermination()
  }

}
