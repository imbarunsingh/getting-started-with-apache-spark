package com.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkConfig {
	
	public static JavaStreamingContext initSparkStreamingContext(int batchSize) {
		SparkConf conf = new SparkConf().setAppName("Spark Streaming Demo")
										.setMaster("local[*]");
		
		//Durations.seconds(30) is the basically the spark streaming batch size
		JavaStreamingContext jsc =  new JavaStreamingContext(conf, Durations.seconds(batchSize));
		return jsc;	
		
	}

}
