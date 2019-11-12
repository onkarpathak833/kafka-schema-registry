package com.kafka.examples;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.kafka.examples.domain.Address;
import com.kafka.examples.domain.Customer;
import com.kafka.examples.domain.Order;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaAccessor<L extends Number, O> {

    private final static String TOPIC = "order";

    private static Producer<Long, Order> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.227.104.191:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, "full");

        props.put("avro.compatibility.level", "full");
        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://35.227.104.191:8081");
        return new KafkaProducer<Long, Order>(props);

    }


    static Schema schemaProvider(Class object) throws JsonMappingException {

        Schema schema = ReflectData.get().getSchema(object);

        return schema;
    }

    public static void main(String... args) {
        Producer<Long, Order> producer = createProducer();
        Customer customer = new Customer(1234, "Onkar Pathak", 25, "Male", true);
        Address shippingAddress = new Address("line1 address", "line 2 address", "MH", "Pune", "IN", 411006);
        Address billingAddress = new Address("line1234 address", "line 2456 address", "MH", "Mumbai", "IN", 40064);

        Order order = new Order(12345, customer, 2754.65, "Shipped", shippingAddress, false);
        IntStream.range(1, 2).forEach(index -> {

            Schema schema = null;
            try {
                schema = schemaProvider(Order.class);
            } catch (JsonMappingException e) {
                e.printStackTrace();
            }
            GenericRecord record = new GenericData.Record(schema);

            ReflectDatumWriter<Order> datumWriter = new ReflectDatumWriter<>(schema);
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
            } catch (IOException e) {
                e.printStackTrace();
            }

            producer.send(new ProducerRecord(TOPIC, 100L * index, record));
        });

        producer.flush();
        producer.close();
    }


}
