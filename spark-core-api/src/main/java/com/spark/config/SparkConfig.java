package com.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConfig {
	
	public static JavaSparkContext initSparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("Spark Core Demo)
											 .setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;
	}
	
	/*
	 * public static SparkSession initSparkSession() { SparkSession sparkSession =
	 * SparkSession.builder() .appName("Spark-SQL")
	 * .config("spark.some.config.option", "some-value") .getOrCreate(); return
	 * sparkSession; }
	 */

}
