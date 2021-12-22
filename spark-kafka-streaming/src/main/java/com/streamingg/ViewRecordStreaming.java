package com.streamingg;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.streaming.config.KafkaConfig;
import com.streaming.config.SparkConfig;

import scala.Tuple2;

public class ViewRecordStreaming {

	public static void main(String[] args) throws InterruptedException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.storage").setLevel(Level.WARN);

		// java streaming context with batch size of 2seconds
		JavaStreamingContext jssc = SparkConfig.initSparkStreamingContext(2);

		Collection<String> topics = Arrays.asList("view-records");
		Map<String, Object> kafkaParams = KafkaConfig.initKafkaParams();

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
																								LocationStrategies.PreferConsistent(),
																								ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaPairDStream<Long, String> results = stream.mapToPair(record -> new Tuple2<>(record.value(), 1L))
													   //generating word counts over the last 30 seconds of data, every 10 seconds
													   // We would see results of last 30 seconds every 10seconds
													  .reduceByKeyAndWindow((x, y) -> x + y, Durations.seconds(30), Durations.seconds(10)) 
													  .mapToPair(item -> item.swap())
													  .transformToPair(rdd -> rdd.sortByKey(false));
		

		results.print();

		jssc.start();
		jssc.awaitTermination();

	}

}
