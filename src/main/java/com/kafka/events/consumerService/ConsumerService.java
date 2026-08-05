package com.kafka.events.consumerService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

public class ConsumerService {
    private static final Logger log = LogManager.getLogger(ConsumerService.class);
    private final KafkaConsumer<String, String> consumer;

     public ConsumerService(){
         this.consumer = createConsumer();
     }

     private KafkaConsumer<String , String> createConsumer(){
            Properties props = new Properties();

            props.setProperty(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092"
            );

             props.setProperty(
                     ConsumerConfig.GROUP_ID_CONFIG,
                     "consumer.group"
             );
            props.setProperty(
                 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                 "true"
            );
            props.setProperty(
                 ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                 "1000"
             );
            props.setProperty(
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringDeserializer"
            );

            props.setProperty(
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringDeserializer"
            );


            return  new KafkaConsumer<>(props);
     }


     public String readTopic(String topicName){
            consumer.subscribe(List.of(topicName));

            for(;;){
                ConsumerRecords<String , String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                log.info("consumer polling: {}" , consumerRecord);
            }
     }

     public void close(){
         consumer.close();
     }



}
