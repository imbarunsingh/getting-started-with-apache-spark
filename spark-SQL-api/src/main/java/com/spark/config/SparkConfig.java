package com.spark.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfig {

	public static SparkSession initSparkSession() {
		SparkSession sparkSession = SparkSession
							  .builder()
							  .appName("Spark SQL Demo")
							  .master("local[*]")
							  .config("spark.sql.warehouse.dir", "some-value")
							  .getOrCreate();
		return sparkSession;
	}

	

}
