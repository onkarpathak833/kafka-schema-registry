package com.kafka.examples;

import com.kafka.examples.domain.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.kafka.examples.constants.Constants.*;

public class KafkaConsumer {

    private static String TOPIC = "";

    private static org.apache.kafka.clients.consumer.KafkaConsumer createConsumer(Properties properties) {
        TOPIC = properties.getProperty(KAFKA_TOPIC);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.227.96.212:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer"); // default topic name
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        // Configure the KafkaAvroSerializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Schema Registry location.
//        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
//                properties.getProperty(SCHEMA_REGISTRY_URL));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        System.out.println(properties.getProperty(KAFKA_BOOTSTRAP_SERVER) + " --> " + properties.getProperty(KAFKA_TOPIC) + " --> " + properties.getProperty(SCHEMA_REGISTRY_URL));
        return new org.apache.kafka.clients.consumer.KafkaConsumer<Long, String>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new Exception("Pass application.config file path..");
        }

        File applicationConfigPath = new File(args[0]);
        Properties properties = new Properties();
        InputStream ins = new FileInputStream(applicationConfigPath);
        try {
            properties.load(ins);
        } catch (Exception e) {
            System.out.println("Error while reading config file");
            e.printStackTrace();
        }

        org.apache.kafka.clients.consumer.KafkaConsumer<Long, String> consumer = createConsumer(properties);
        consumer.subscribe(Collections.singletonList("test"));
        System.out.println("Subscribed to the topic here..." + "test" + "-1");
        int records = consumer.poll(Duration.ofMinutes(1)).count();
        System.out.println("No of records : "+records);
        if(records!=0) {

            for(int i = 0;i< records;i++) {

            }
        }
        consumer.poll(20).forEach(record -> {
            System.out.println("Consuming records...");
            System.out.println(record.key() + " --> "+record.value());
            System.out.println(record.partition()+ " --> "+record.offset());
        });

        consumer.commitAsync();
        consumer.close();
    }

}
