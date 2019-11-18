package com.kafka.examples;

import com.kafka.examples.domain.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import static com.kafka.examples.constants.Constants.*;

public class KafkaConsumer {

    private static String TOPIC = "";

    private static org.apache.kafka.clients.consumer.KafkaConsumer createConsumer(Properties properties) {
        TOPIC = properties.getProperty(KAFKA_TOPIC);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(KAFKA_BOOTSTRAP_SERVER));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, PRODUCER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(SCHEMA_REGISTRY_URL));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new org.apache.kafka.clients.consumer.KafkaConsumer<Long, Order>(props);
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

        org.apache.kafka.clients.consumer.KafkaConsumer<Long, Order> consumer = createConsumer(properties);

        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("Subscribed to the topic here..."+TOPIC+"-1");
        consumer.poll(2000).forEach(record -> {
            System.out.println("Consuming records...");
            System.out.println(record.key() + " ---> " + record.value());
        });

        consumer.commitAsync();
        consumer.close();
    }

}
