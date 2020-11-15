package com.server;

import com.dto.KafkaMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class KafkaDiApplication {


    public static void main(String[] args) {
        SpringApplication.run(KafkaDiApplication.class, args);
    }

    @RestController
    @RequestMapping("api/v1")
    public static class PublishController {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        @Autowired
        private KafkaTemplate<String, KafkaMessage> messageKafkaTemplate;

        @RequestMapping(value = "/publish/{message}", method = RequestMethod.GET)
        public void publish(@PathVariable("message") String message) {
            for (int i = 0; i < 10; i++) {
                send("partionedTopic", "partionedTopic___" + message + "___" + i);

                KafkaMessage kafkaMessage = new KafkaMessage();
                kafkaMessage.setId(i);
                kafkaMessage.setName(String.valueOf(i));
                kafkaMessage.setValue(String.valueOf(i + 1));
                send("topic2", kafkaMessage);

                //send("partionedTopic1", "partionedTopic1___" + message + "___" + i);
            }
        }

        private void send(String topic, Object message) {
            ListenableFuture<SendResult<String, String>> listenableFuture =
                kafkaTemplate.send(topic, message + "_" + System.currentTimeMillis());
            listenableFuture.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }
                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }

        private void send(String topic, KafkaMessage message) {
            ListenableFuture<SendResult<String, KafkaMessage>> listenableFuture = messageKafkaTemplate.send(topic, message);
            listenableFuture.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, KafkaMessage> result) {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }
                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }
    }
}
