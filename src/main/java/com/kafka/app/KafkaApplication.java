package com.kafka.app;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

		//Defining Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//Creating Producer
		KafkaProducer producer = new KafkaProducer(properties);

		for(int i =0; i< 10; i++) {
			//Creating Producer Record
			ProducerRecord record = new ProducerRecord("first_topic", "formatting message output -" + Integer.toString(i));

			//Sending Message
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					//going to execute everytime record is successfully sent
					if (exception != null) {
						// there was some issue with the record
						logger.error("Error while producing", exception);
					} else {
						//record was successfully send
						logger.info("\n Recieved new metadata. \t" +
								"Topic: " + metadata.topic() + "\t" +
								"Partition: " + metadata.partition() + "\t" +
								"Offset: " + metadata.offset() + "\t" +
								"Timestamp: " + metadata.timestamp());
					}
				}
			});
		}

		SpringApplication.run(KafkaApplication.class, args);
	}

}
