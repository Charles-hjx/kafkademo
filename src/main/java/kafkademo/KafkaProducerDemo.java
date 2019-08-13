package kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.naming.ldap.ControlFactory;
import java.util.Properties;

/**
 * @Author: hjx
 * @Date: 2019/8/10
 * @Version 1.0
 */
public class KafkaProducerDemo extends Thread {

    public final KafkaProducer<Integer,String> producer;

    public final String topic;

    public KafkaProducerDemo(String topic){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.127.132:9092,192.168.127.133:9092,192.168.127.134:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerDemo");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(properties);

        this.topic = topic;
    }



    @Override
    public void run() {
        int num = 0;
        while(num < 50){
            num++;
            String message ="message-"+ num;
            producer.send(new ProducerRecord<Integer, String>(topic,message));
            try {
                sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        KafkaProducerDemo demo = new KafkaProducerDemo("test2");
        demo.start();
    }

}






