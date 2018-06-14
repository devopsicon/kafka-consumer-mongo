package org.devopsicon.kafka.consumer.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {

    public static void main(String[] args) {
        String topicName = "AvroClicks";
        String groupName = "RG";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");

        //DB connection
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        DB db = mongoClient.getDB( "testDB" );

        DBCollection coll = db.getCollection("salesTestCollection");

        try(KafkaConsumer<String, Sale> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topicName));
            while(true) {
                ConsumerRecords<String, Sale> records = consumer.poll(100);
                for(ConsumerRecord<String, Sale> record: records) {
                    System.out.println("Amount="+record.value().getAmount());
                }
            }
        }

    }
}
