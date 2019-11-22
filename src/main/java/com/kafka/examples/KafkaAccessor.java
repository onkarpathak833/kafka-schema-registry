package com.kafka.examples;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.kafka.examples.domain.Address;
import com.kafka.examples.domain.Customer;
import com.kafka.examples.domain.Order;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;
import java.util.Random;

import static com.kafka.examples.constants.Constants.*;

public class KafkaAccessor<L extends Number, O> {

    private static String TOPIC = "";

    private static KafkaConsumer createConsumer(Properties properties) {
        TOPIC = properties.getProperty(KAFKA_TOPIC);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(KAFKA_BOOTSTRAP_SERVER));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, PRODUCER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(SCHEMA_REGISTRY_URL));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<Long, Order>(props);
    }


    private static KafkaProducer<String, String> createSimpleProducer(Properties properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(KAFKA_BOOTSTRAP_SERVER));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);

    }

    private static KafkaProducer<Long, Order> createProducer(Properties properties) {
        TOPIC = properties.getProperty(KAFKA_TOPIC);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(KAFKA_BOOTSTRAP_SERVER));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, properties.getProperty(AVRO_COMPATIBILITY));

        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class);
        props.put(KafkaAvroSerializerConfig.KEY_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class);
        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(SCHEMA_REGISTRY_URL));
        return new KafkaProducer<Long, Order>(props);

    }


    private static Schema schemaProvider(Class object) throws JsonMappingException {

        Schema schema = ReflectData.get().getSchema(object);

        return schema;
    }

    private static void generateKafkaMessages(KafkaProducer<String, String> producer) {

        for (int i = 0; i < 20; i++) {
            String randomString = "Hello World there -->" + i;
            ProducerRecord record = new ProducerRecord("test", String.valueOf(i * 12), randomString);
            System.out.println("Produced Message : " + record);
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }

    private static void generateGenericRecordMessages(KafkaProducer<Long, Order> producer) {
        Order order = getOrder();

        for (int i = 0; i < 1; i++) {

            int randomKey = new Random().nextInt(188999);
            Schema schema = null;
            try {
                schema = schemaProvider(Order.class);
            } catch (JsonMappingException e) {
                e.printStackTrace();
            }
            GenericRecord record = new GenericData.Record(schema);

            GenericDatumWriter<Order> datumWriter = new ReflectDatumWriter<>(schema);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            try {
                datumWriter.write(order, encoder);
                encoder.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }


            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);


            try {
                record = datumReader.read(null, decoder);
                System.out.println("Record Generated is : " + record);
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Publishing message " + i + " on topic --> " + record.getSchema().toString());
            producer.send(new ProducerRecord(TOPIC, (100L * i) + randomKey, record));


        }
        producer.flush();
        producer.close();
    }

    private static Order getOrder() {
        Customer customer = new Customer(1234, "Onkar Pathak", 25, "Male", true);
        Address shippingAddress = new Address("line1 address", "line 2 address", "MH", "Pune", "IN", 411006);
        Address billingAddress = new Address("line1234 address", "line 2456 address", "MH", "Mumbai", "IN", 40064);
        return new Order(12345, customer, 2754.65, "Shipped", shippingAddress, false);
    }


    public static void main(String args[]) throws Exception {

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


        try {
            KafkaProducer<String, String> producer = createSimpleProducer(properties);
            //generateKafkaMessages(producer);

            KafkaProducer<Long, Order> avroProducer = createProducer(properties);
            generateGenericRecordMessages(avroProducer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
