package org.sample.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimideaChangeHandler implements EventHandler {
    KafkaProducer<String,String> KafkaProducer;
    String topic;
    private final Logger log= LoggerFactory.getLogger(WikimideaChangeHandler.class.getSimpleName());
    public WikimideaChangeHandler(KafkaProducer<String,String> KafkaProducer,String topic){
        this.KafkaProducer=KafkaProducer;
        this.topic=topic;
    }
    @Override
    public void onOpen()  {
        //nothing
    }

    @Override
    public void onClosed() {
        KafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent)  {
        log.info(messageEvent.getData());
        KafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading ",t);
    }
}
