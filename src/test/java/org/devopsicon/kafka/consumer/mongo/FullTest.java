package org.devopsicon.kafka.consumer.mongo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Properties;

@RunWith(JUnit4.class)
public class FullTest {

    @Test
    public void fullTest() {
        String topicName = "AvroClicks";
        String msg;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");


        Sale sale = new Sale();

        sale.setAmount(100.0f);
        try(org.apache.kafka.clients.producer.Producer<String, Sale> producer = new KafkaProducer<>(props)){
            producer.send(new ProducerRecord<>(topicName, sale));
        }
    }
}
