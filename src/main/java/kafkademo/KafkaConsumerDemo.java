package kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: hjx
 * @Date: 2019/8/10
 * @Version 1.0
 */
public class KafkaConsumerDemo extends Thread {

    public final KafkaConsumer<Integer,String> consumer;


    public KafkaConsumerDemo(String topic){

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.127.132:9092,192.168.127.133:9092,192.168.127.134:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"KafkaProducerDemo3");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<Integer, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }



    @Override
    public void run() {
        while(true){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000L));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("消费者拿到的消息："+record.value());
            }
        }
    }

    public static void main(String[] args) {
        KafkaConsumerDemo demo = new KafkaConsumerDemo("test2");
        demo.start();
    }

}






