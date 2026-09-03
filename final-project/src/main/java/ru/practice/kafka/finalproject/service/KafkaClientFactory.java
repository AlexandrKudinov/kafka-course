package ru.practice.kafka.finalproject.service;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaClientFactory {
    private final KafkaProperties properties;

    public KafkaClientFactory(KafkaProperties properties) {
        this.properties = properties;
    }

    public Properties producerProperties(String bootstrap, String keystore) {
        Properties p = common(bootstrap, keystore);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, 10);
        return p;
    }

    public Properties plainConsumerProperties(String bootstrap, String groupId) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public Properties consumerProperties(String bootstrap, String keystore, String groupId) {
        Properties p = common(bootstrap, keystore);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public Map<String, Object> streamsProperties() {
        Map<String, Object> p = new HashMap<>();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, properties.primaryBootstrap);
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.truststore);
        p.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, properties.streamsKeystore);
        p.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
        p.put("application.id", "final-project-filter");
        p.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        p.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        return p;
    }

    private Properties common(String bootstrap, String keystore) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.truststore);
        p.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        p.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
        p.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, properties.password);
        p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        p.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
        return p;
    }
}
