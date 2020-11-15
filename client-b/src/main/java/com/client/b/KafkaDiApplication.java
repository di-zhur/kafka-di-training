package com.client.b;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class KafkaDiApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDiApplication.class, args);
	}

	@KafkaListener(topics = "partionedTopic")
    public void listen(String message) {
        System.out.println(message);
    }

    @KafkaListener(topics = "partionedTopic1")
    public void listen1(String message) {
        System.out.println(message);
    }
}
