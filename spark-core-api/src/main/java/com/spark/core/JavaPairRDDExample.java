package com.spark.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import com.spark.config.SparkConfig;

import scala.Tuple2;

public class JavaPairRDDExample {
	public static void main(String[] args) {
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tueseday 4 September 0405");
		inputData.add("ERROR: Monday 4 September 0408");
		inputData.add("FATAL: Wednesday 4 September 1632");
		inputData.add("ERROR: Tueseday 4 September 0405");
		inputData.add("WARN: Friday 7 September 0405");
		inputData.add("WARN: Tueseday 4 September 0405");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		JavaSparkContext sparkContext = SparkConfig.initSparkContext();
		
		JavaRDD<String> originalMessages = sparkContext.parallelize(inputData);
		
			
		JavaPairRDD<String, Long> pairRDD = originalMessages.mapToPair(value -> {
			String columns[] = value.split(":");
			String level = columns[0];
			
			return new Tuple2<>(level, 1L);
		});		
		
		//JavaPairRDD<String, Iterable<Long>> groupedPairRDD = pairRDD.groupByKey(); // Not recommended in prod on cluster set up. Can cause issue.
		
		JavaPairRDD<String, Long> sumsRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
		
		sumsRDD.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
				
		sparkContext.close();
		
	}

}
