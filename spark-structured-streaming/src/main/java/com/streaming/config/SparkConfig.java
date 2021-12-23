package com.streaming.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfig {

	public static SparkSession initSparkSession() {
		SparkSession sparkSession = SparkSession
							  .builder()
							  .appName("Spark Structured Streaming")
							  .master("local[*]")
							  .getOrCreate();
		return sparkSession;
	}
}
