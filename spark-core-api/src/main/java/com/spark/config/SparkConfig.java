package com.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConfig {
	
	public static JavaSparkContext initSparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("startingSpark")
											 .setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;
	}

}
