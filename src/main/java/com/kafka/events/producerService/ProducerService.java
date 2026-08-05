package com.kafka.events.producerService;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ProducerService {

        private final KafkaProducer<String , String> kafkaProducer;

        public ProducerService(){
            this.kafkaProducer = createProducer();
        }

        private KafkaProducer<String , String> createProducer(){
            Properties props = new Properties();

            props.setProperty(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.setProperty(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer"
            );
            props.setProperty(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer"
            );

            return new KafkaProducer<>(props);
        }

        public void sendMessage(String topicName ,String key, String message){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,key , message);
            kafkaProducer.send(producerRecord);
        }

        public  void close(){
            kafkaProducer.close();
        }



}
