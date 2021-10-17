package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

public class FlatMapDemo {
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
		
		JavaRDD<String> sentences = sparkContext.parallelize(inputData);
		
		JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		words.foreach(value -> System.out.println(value)); //test code
		
		
		
				
		sparkContext.close();
		
	}

}
