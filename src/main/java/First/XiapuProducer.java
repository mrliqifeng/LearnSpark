package First;

import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author liqifeng
 * 此类使用Holder单例模式实现了kafka生产者
 */
public class XiapuProducer {
    private static Producer<String, String> producer;
    private static Logger log =Logger.getLogger(XiapuProducer.class);
    private static class XiapuProducerHolder{
        private static XiapuProducer xiapuProducer = new XiapuProducer();
    }

    private XiapuProducer(){
        log.info("Init Class XiapuProducer...");
        Properties props = new Properties();
        props.setProperty("retries","0");
        props.setProperty("zookeeper.connect","localhost:2181");
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("linger.ms","1");
        props.setProperty("acks","all");
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("buffer.memory","33554432");
        producer = new KafkaProducer<>(props);
        log.info("Init Class XiapuProducer success");
    }

    public static XiapuProducer getInstance(){
        return XiapuProducerHolder.xiapuProducer;
    }

    /**
     * 调用此方法发送消息，
     * @param msg 待发送的用户行为数据，格式为JSON格式，使用时需将JSON对象转化为String对象
     */
    public void send(String msg){
        final ProducerRecord<String, String>record = new ProducerRecord<String, String>("spark",msg);
        //发送消息，并且调用回调函数，并对返回的偏移量信息进行操作
        producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        /*
                         * 如果一分钟后未返回偏移量，则会报超时错误。错误如下
                         * org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.
                         * 此错误多是由于生产者代码连接kafka失败所导致
                         * 第一，查看kafka是否启动，如果未启动，则启动kafka即可解决
                         * 第二，如果kafka正在运行，则检查上述配置项中zookeeper.connect和value.serializer是否配置正确
                         */
                        if(e != null){
                            e.printStackTrace();
                        } else{
                            log.info(String.format("record-info:%s-%d-%d, send successfully, value is %s",recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), record.value()));
                        }
                    }
                });
        //此步骤非常重要，如果省略，会造成消息阻塞，消费者无法接受到消息
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("lll");
        XiapuProducer xiapuProducer = XiapuProducer.getInstance();
        System.out.println("lll2");
        String result = "{\"uuid\":\"uuid\",\"header\":{\"produceTime\":1644622393585,\"sendTime\":1544622393599,\"systemName\":\"xiapu\",\"serverIp\":\"192.168.0.1\",\"clientIp\":\"192.168.7.34\",\"userId\":\"liqifeng\",\"userType\":\"student\"},\"body\":{\"eventType\":\"pc\",\"eventName\":\"login\",\"num\":\"1\"}}";//,"resourceName":"古典文学.txt"
        JSONObject json = JSONObject.fromObject(result);
            for(int i=31000;i<32000;i++){
//                log.info(json);
                json.put("uuid",String.format("uuid%d",i));
                xiapuProducer.send(json.toString());
                System.out.println(json.toString());
                Thread.sleep(1000);
        }
    }

}
