package ru.practice.kafka.finalproject.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;
import ru.practice.kafka.finalproject.service.KafkaClientFactory;
import ru.practice.kafka.finalproject.service.KafkaProperties;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
public class ProductFilterTopology {
    private final KafkaClientFactory factory;
    private final KafkaProperties kafka;

    public ProductFilterTopology(KafkaClientFactory factory, KafkaProperties kafka) {
        this.factory = factory;
        this.kafka = kafka;
    }

    public void startAndWait() throws InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> forbidden = builder.globalTable(
                kafka.forbiddenTopic,
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()),
                org.apache.kafka.streams.kstream.Materialized.as("forbidden-products-store"));

        KStream<String, String> products = builder.stream(
                kafka.productsTopic,
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()));

        products.leftJoin(
                        forbidden,
                        (key, value) -> key,
                        (product, marker) -> marker == null ? product : null)
                .filter((key, value) -> value != null)
                .to(kafka.filteredTopic, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));

        var config = new java.util.HashMap<String, Object>(factory.streamsProperties());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "final-project-filter");
        config.put(StreamsConfig.STATE_DIR_CONFIG, "final-project/state-store");
        KafkaStreams streams = new KafkaStreams(builder.build(), new java.util.Properties() {{
            putAll(config);
        }});
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Kafka Streams uncaught exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info("Product filtering topology started");
        new CountDownLatch(1).await();
    }
}
