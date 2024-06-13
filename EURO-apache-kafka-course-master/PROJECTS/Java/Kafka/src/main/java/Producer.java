import org.apache.kafka.clients.producer.*; // Import Kafka producer classes

import java.util.Date;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String clientId = "my-producer"; // Define the client ID for the producer

        // Set up properties for the Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094"); // Kafka broker addresses
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer for the message key
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer for the message value
        props.put("acks", "all"); // Acknowledgement setting (wait for all replicas to acknowledge)
        props.put("client.id", clientId); // Set the client ID for the producer

        // Create a Kafka producer with the specified properties
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int numOfRecords = 100; // Number of messages to send
        String topic = "numbers"; // Topic name to send messages to

        // EXAMPLE 1 - Send numbers as strings for key and value without any delay
        for (int i = 0; i < numOfRecords; i++) {
            System.out.println("Message " + i + " was just sent"); // Log message to console
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i))); // Send message to Kafka
        }
        producer.close(); // Close the producer to release resources

        // EXAMPLE 2 - Send formatted string messages with a 300ms delay (3 messages per second)
        /*
        try {
            for (int i = 0; i < numOfRecords; i++) {
                String message = String.format("Producer %s has sent message %s at %s", clientId, i, new Date()); // Create a formatted message string
                System.out.println(message); // Log message to console
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), message)); // Send message to Kafka
                Thread.sleep(300); // Sleep for 300ms between messages
            }
        } catch (Exception e) {
            e.printStackTrace(); // Print stack trace if an exception occurs
        } finally {
            producer.close(); // Ensure the producer is closed to release resources
        }
        */
    }
}
