package com.prodapt.config;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.google.gson.JsonDeserializer;

@EnableKafka
@Configuration
public class KafkaConsumerConfigs {
	public static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfigs.class);

	@Bean
	public Properties consumerConfigs() {
		logger.info("----------------->Logged IN <-------------------------------------");
		final Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "NetBots");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500000");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	 	props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		props.put(SaslConfigs.SASL_JAAS_CONFIG,
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"DQA2B4WNZDSUWOB5\" password=\"wff/UnO1AwY9dGCuwLH1U7AdLejgRV4U6TBBNcypzaM/pV3/mshgpEPzajiuOasC\";");

		return props;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		logger.info("----------------->consumerFactory IN <-------------------------------------");
		Map<String, Object> props = new HashMap<>();
		for (Entry<Object, Object> entry : consumerConfigs().entrySet()) {
			props.put((String) entry.getKey(), entry.getValue());
		}

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory()
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		logger.info("----------------->kafkaListenerContainerFactory IN <-------------------------------------");
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

//	@Bean
//	public void sam() {
//		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.consumerConfigs());
//		consumer.subscribe(Arrays.asList("fcaps-uat-alarms-active"));
//		while (true) {
//			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(20));
//			for (ConsumerRecord<String, String> record : records) {
//
//				String key = "key = " + record.key();
//				String value = " value = " + record.value();
//				logger.info(key, value);
//			}
//		}
	// }

}
