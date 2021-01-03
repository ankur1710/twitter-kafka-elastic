package com.twitter.kafkaconsumerelastic;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;

@SpringBootApplication
public class KafkaConsumerElasticApplication implements CommandLineRunner {

	private static JsonParser jsonParser = new JsonParser();

	private static Logger logger = LoggerFactory.getLogger(KafkaConsumerElasticApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerElasticApplication.class, args);
	}

	@Autowired
	ElasticSearchConsumer elasticSearchConsumer;

	@Autowired
	TwitterConsumerKafka twitterConsumerKafka;

	@Override
	public void run(String... args) throws Exception {
		RestHighLevelClient esRestClient = elasticSearchConsumer.getElasticSearchClient();

		//KafkaConsumer
		Consumer<String,String> twitterConsumerEs = twitterConsumerKafka.getKafkaConsumer("twitter_tweets");

		while(true) {
			ConsumerRecords<String, String> twitterRecords = twitterConsumerEs.poll(Duration.ofMillis(100));
			logger.info("records received : {}" + twitterRecords.count());

			for (ConsumerRecord<String, String> record : twitterRecords) {
				String id = extractTwitterIdfromJson(record.value());

				//insert the details in the elasticSearch
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);
				IndexResponse indexResponse = esRestClient.index(indexRequest, RequestOptions.DEFAULT);
				logger.info(indexResponse.getId());
				Thread.sleep(1000); // this is to introduce a small delay.
			}
			logger.info("Committing offsets.....");
			twitterConsumerEs.commitAsync();
			logger.info("offsets have been committed");
			Thread.sleep(1000); // this is to introduce a small delay.
		}
	}

	private String extractTwitterIdfromJson(String tweetsJson) {
		return jsonParser.parse(tweetsJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();

	}
}
