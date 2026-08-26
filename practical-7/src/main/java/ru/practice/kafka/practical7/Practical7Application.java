package ru.practice.kafka.practical7;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@Slf4j
@SpringBootApplication
public class Practical7Application {

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(Practical7Application.class, args);
    }

    @Bean
    CommandLineRunner run(KafkaMessageProducer producer, KafkaMessageConsumer consumer) {
        return args -> {
            if (args.length > 0 && "consume".equalsIgnoreCase(args[0])) {
                consumer.consumeTestMessages();
            } else if (args.length > 0 && "produce".equalsIgnoreCase(args[0])) {
                producer.sendTestMessages();
            } else {
                log.info("Use: ./gradlew :practical-7:bootRun --args='produce'");
                log.info("or:  ./gradlew :practical-7:bootRun --args='consume'");
            }
        };
    }
}
