package com.bharath.kafka.avro.deserializers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.bharath.kafka.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class GenericOrderConsumer {

    public static void main(String[] args) {
        // Set up properties for Kafka consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092"); // Specify Kafka broker address
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName()); // Specify key deserializer
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName()); // Specify value deserializer
        props.setProperty("group.id", "OrderGroup"); // Specify consumer group ID
        props.setProperty("schema.registry.url", "http://localhost:8081"); // Specify schema registry URL

        // Create a Kafka consumer with the specified properties
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic "OrderAvroGRTopic"
        consumer.subscribe(Collections.singletonList("OrderAvroGRTopic"));

        // Poll for records from the Kafka topic
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(20));
        // Iterate over each record and process it
        for (ConsumerRecord<String, GenericRecord> record : records) {
            String customerName = record.key(); // Get the key (customer name) from the record
            GenericRecord order = record.value(); // Get the value (order details) from the record
            // Print the customer name and order details
            System.out.println("Customer Name: " + customerName);
            System.out.println("Product: " + order.get("product"));
            System.out.println("Quantity: " + order.get("quantity"));
        }
        // Close the consumer
        consumer.close();
    }
}
