package com.spark.core;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

public class LoadAllFileInDirectory {
	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		JavaSparkContext sparkContext = SparkConfig.initSparkContext();

		sparkContext.textFile("src/main/resources/loadallfiles/*")
					.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
					.filter(value -> value.length() > 4)
					.foreach(value -> System.out.println(value));

		sparkContext.close();

	}

}
