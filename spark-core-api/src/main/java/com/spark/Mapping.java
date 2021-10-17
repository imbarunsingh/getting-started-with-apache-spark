package com.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

public class Mapping {
	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(144);
		inputData.add(25);
		inputData.add(90);		
		
		JavaSparkContext sparkContext = SparkConfig.initSparkContext();
		
		JavaRDD<Integer> javaRDD = sparkContext.parallelize(inputData);
		
			
		JavaRDD<Tuple2<Integer, Double>> sqrtRDD = javaRDD.map(value -> new Tuple2<>(value, Math.sqrt(value)));		
		sqrtRDD.foreach(x -> System.out.println("Mapped Result : " + x)); //for testing purpose on local mode only
				
				
		sparkContext.close();
		
	}

}
