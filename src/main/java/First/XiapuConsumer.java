package First;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class XiapuConsumer {
    private static class XiapuConsumerHolder{
        private static XiapuConsumer xiapuConsumer = new XiapuConsumer();
    }

    private XiapuConsumer(){
        Properties props = new Properties();
        props.setProperty("zookeeper.connect","localhost:2181");
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("enable.auto.commit","false");
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("group.id","xiapu");
        props.setProperty("session.timeout.ms","30000");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //指定topic信息
        consumer.subscribe(Collections.singletonList("xiapu_test2"));
        while(true){
            //接收消息，poll参数为连接超时时间
            ConsumerRecords<String, String> records = consumer.poll(6000);
            for(ConsumerRecord<String, String> record:records){
                JSONObject jsonObject = JSONObject.parseObject(record.value());
                //如果写入Hbase成功，则手动提交偏移量
                consumer.commitAsync();
                System.out.println(String.format("record-info:%s-%d-%d, writes to Hbase successfully, value is %s", record.topic(),record.partition(), record.offset(), record.value()));
            }
        }
    }

    public static XiapuConsumer getInstance(){
        return XiapuConsumerHolder.xiapuConsumer;
    }

    public static void main(String[] args) {
        XiapuConsumer xiapuConsumer = new XiapuConsumer();
    }

}
