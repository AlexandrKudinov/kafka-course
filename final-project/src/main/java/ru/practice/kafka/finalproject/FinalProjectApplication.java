package ru.practice.kafka.finalproject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.practice.kafka.finalproject.service.AnalyticsService;
import ru.practice.kafka.finalproject.service.ClientApiService;
import ru.practice.kafka.finalproject.service.ForbiddenProductService;
import ru.practice.kafka.finalproject.service.KafkaReplicationService;
import ru.practice.kafka.finalproject.service.ShopApiService;
import ru.practice.kafka.finalproject.stream.ProductFilterTopology;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class FinalProjectApplication implements CommandLineRunner {

    private final ShopApiService shopApiService;
    private final ClientApiService clientApiService;
    private final ForbiddenProductService forbiddenProductService;
    private final ProductFilterTopology productFilterTopology;
    private final KafkaReplicationService replicationService;
    private final AnalyticsService analyticsService;

    public static void main(String[] args) {
        SpringApplication.run(FinalProjectApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        switch (args[0].toLowerCase()) {
            case "shop" -> shopApiService.publishProducts();
            case "stream" -> productFilterTopology.startAndWait();
            case "forbid" -> forbiddenProductService.forbid(requireArg(args));
            case "allow" -> forbiddenProductService.allow(requireArg(args));
            case "search" -> clientApiService.search(requireArg(args));
            case "recommend" -> clientApiService.recommend(requireArg(args));
            case "replicate" -> replicationService.startAndWait();
            case "analytics" -> analyticsService.startAndWait();
            default -> printUsage();
        }
    }

    private String requireArg(String[] args) {
        if (args.length < 2 || args[1].isBlank()) {
            throw new IllegalArgumentException("Argument is required");
        }
        return args[1];
    }

    private void printUsage() {
        log.info("Commands:");
        log.info("shop                  - read data/products.json and publish products");
        log.info("stream                - start Kafka Streams filtering topology");
        log.info("forbid <productId>    - add product to forbidden list");
        log.info("allow <productId>     - remove product from forbidden list");
        log.info("search <name>         - search the JSON data and publish the search event");
        log.info("recommend <category>  - read a ready recommendation (consumer only)");
        log.info("replicate             - copy filtered products to the second Kafka cluster");
        log.info("analytics             - analyze the second cluster and publish recommendations");
    }
}
