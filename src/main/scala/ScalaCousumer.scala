import java.util.{Collections, Properties}

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}

object ScalaCousumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("enable.auto.commit", "false")
    props.setProperty("auto.offset.reset", "earliest")
    props.setProperty("group.id", "xiapu")
    props.setProperty("session.timeout.ms", "30000")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    //指定topic信息
    consumer.subscribe(Collections.singletonList("xiapu_test2"))
    while (true) { //接收消息，poll参数为连接超时时间
      val records = consumer.poll(6000)
      import scala.collection.JavaConversions._
      for (record <- records) {
        val jsonObject = JSON.parseObject(record.value)
        //如果写入Hbase成功，则手动提交偏移量
        consumer.commitAsync()
        System.out.println(record.topic+"--"+record.partition+"--"+record.offset+"--"+record.value)
      }
    }
  }
}
