package org.sample.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka producer");
        //create producer props
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String topic="demo_java";
            String value= "hello world "+ i;
            String key="id_"+i;
            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key,value);
            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time producer successfully sends data or an exception is thrown
                    if (exception == null) {
                        //the data was successfully sent
                        log.info("Received new metadata/ \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "TimeStamp: " + metadata.timestamp());
                    } else {
                        log.error("Error in while producing ", exception);
                    }
                }
            });
            try {
                Thread.sleep(1000);
            }
            catch (Exception e){}
        }
        //flush and close producer
        producer.flush();
        producer.close();
    }
}
