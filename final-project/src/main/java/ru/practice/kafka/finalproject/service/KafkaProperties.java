package ru.practice.kafka.finalproject.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaProperties {
    public final String primaryBootstrap;
    public final String secondaryBootstrap;
    public final String truststore;
    public final String shopKeystore;
    public final String clientKeystore;
    public final String streamsKeystore;
    public final String analyticsKeystore;
    public final String password;

    public final String productsTopic;
    public final String forbiddenTopic;
    public final String filteredTopic;
    public final String clientRequestsTopic;
    public final String recommendationsTopic;

    public KafkaProperties(
            @Value("${kafka.primary.bootstrap-servers}") String primaryBootstrap,
            @Value("${kafka.secondary.bootstrap-servers}") String secondaryBootstrap,
            @Value("${kafka.ssl.truststore-location}") String truststore,
            @Value("${kafka.ssl.shop-keystore-location}") String shopKeystore,
            @Value("${kafka.ssl.client-keystore-location}") String clientKeystore,
            @Value("${kafka.ssl.streams-keystore-location}") String streamsKeystore,
            @Value("${kafka.ssl.analytics-keystore-location}") String analyticsKeystore,
            @Value("${kafka.ssl.password}") String password,
            @Value("${kafka.topics.products}") String productsTopic,
            @Value("${kafka.topics.forbidden-products}") String forbiddenTopic,
            @Value("${kafka.topics.filtered-products}") String filteredTopic,
            @Value("${kafka.topics.client-requests}") String clientRequestsTopic,
            @Value("${kafka.topics.recommendations}") String recommendationsTopic) {
        this.primaryBootstrap = primaryBootstrap;
        this.secondaryBootstrap = secondaryBootstrap;
        this.truststore = truststore;
        this.shopKeystore = shopKeystore;
        this.clientKeystore = clientKeystore;
        this.streamsKeystore = streamsKeystore;
        this.analyticsKeystore = analyticsKeystore;
        this.password = password;
        this.productsTopic = productsTopic;
        this.forbiddenTopic = forbiddenTopic;
        this.filteredTopic = filteredTopic;
        this.clientRequestsTopic = clientRequestsTopic;
        this.recommendationsTopic = recommendationsTopic;
    }
}
