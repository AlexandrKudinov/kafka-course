package ru.practice.kafka.practical6.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaSecurityConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.security-protocol}")
    private String securityProtocol;

    @Value("${kafka.truststore-location}")
    private String truststoreLocation;

    @Value("${kafka.truststore-password}")
    private String truststorePassword;

    @Value("${kafka.endpoint-identification-algorithm}")
    private String endpointIdentificationAlgorithm;

    @Value("${kafka.producer.keystore-location}")
    private String producerKeystoreLocation;

    @Value("${kafka.producer.keystore-password}")
    private String producerKeystorePassword;

    @Value("${kafka.producer.key-password}")
    private String producerKeyPassword;

    @Value("${kafka.consumer.keystore-location}")
    private String consumerKeystoreLocation;

    @Value("${kafka.consumer.keystore-password}")
    private String consumerKeystorePassword;

    @Value("${kafka.consumer.key-password}")
    private String consumerKeyPassword;

    private Map<String, Object> common() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentificationAlgorithm);
        return props;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> props = common();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, producerKeystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, producerKeystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, producerKeyPassword);
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> props = common();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, consumerKeystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, consumerKeystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, consumerKeyPassword);
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        return factory;
    }
}
