package com.client.a;

import com.dto.KafkaMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class KafkaDiApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDiApplication.class, args);
	}

    @KafkaListener(topics = "partionedTopic")
    public void listen(String message) {
        System.out.println(message);
    }

    @KafkaListener(topics = "topic2", containerFactory = "kafkaMessageKafkaListenerFactory")
    public void listen2(KafkaMessage message) {
        System.out.println("listen2 - " + message);
    }

    @KafkaListener(topics = "topic2", containerFactory = "kafkaMessageKafkaListenerFactory")
    public void listen3(KafkaMessage message) {
        System.out.println("listen3 - " + message);
    }

    private AtomicInteger counter = new AtomicInteger(0);

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topicTest2", partitions = { "0" }),
            containerFactory = "kafkaMessageKafkaListenerFactory")
    public void listenTopicTest1(KafkaMessage message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("listenTopicTest1 - " + message + " from partition: " + partition);
        System.out.println(counter.incrementAndGet());
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topicTest2", partitions = { "1" }),
            containerFactory = "kafkaMessageKafkaListenerFactory")
    public void listenTopicTest2(KafkaMessage message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("listenTopicTest2 - " + message + " from partition: " + partition);
        System.out.println(counter.incrementAndGet());
    }

    @KafkaListener(topics = "topicTest2",
            containerFactory = "kafkaMessageKafkaListenerFactory")
    public void listenTopicTest3(KafkaMessage message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("listenTopicTest3 - " + message + " from partition: " + partition);
        System.out.println(counter.incrementAndGet());
    }

}
