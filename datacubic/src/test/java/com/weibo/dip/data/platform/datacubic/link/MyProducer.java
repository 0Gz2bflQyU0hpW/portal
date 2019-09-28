package com.weibo.dip.data.platform.datacubic.link;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MyProducer {

	public static void main(String[] args) {
		Map<String, Object> config = new HashMap<>();

		config.put("bootstrap.servers", "10.210.136.61:9092");

		config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(config);

		String topic = "test2"; //kafka创建的topic

		Random random = new Random();

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

		while (true) {
			String key = String.valueOf(random.nextInt());

			String value = "row" + random.nextInt(3) + " row" + random.nextInt(3) + " " + random.nextInt(3) + " "
					+ dateFormat.format(new Date(System.currentTimeMillis()));

			producer.send(new ProducerRecord<String, String>(topic, key, value));

			if (Thread.interrupted()) {
				break;
			}

		}

		producer.close();

	}
}
