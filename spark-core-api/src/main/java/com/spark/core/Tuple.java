package com.spark.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

public class Tuple {
	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(144);
		inputData.add(25);
		inputData.add(90);		
		
		JavaSparkContext sparkContext = SparkConfig.initSparkContext();
		
		JavaRDD<Integer> javaRDD = sparkContext.parallelize(inputData);
		
		Integer resultOnReduce = javaRDD.reduce((value1, value2) -> value1 + value2);		
		System.out.println("Resultant Sum through reduce : " + resultOnReduce);
		
		JavaRDD<Double> sqrtRDD = javaRDD.map(value -> Math.sqrt(value));		
		sqrtRDD.foreach(x -> System.out.println("Mapped Result : " + x)); //for testing purpose on local mode only
		
		Tuple2<Integer, Double> tuple = new Tuple2<>(9, 3.0);
		
		//count on rdd
		System.out.println("Result Count : " +sqrtRDD.count());
				
		sparkContext.close();
		
	}

}
