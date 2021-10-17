package com.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Reduce {

	public static void main(String[] args) {		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(35.5);
		inputData.add(102.5);
		inputData.add(87.26);
		inputData.add(89.5);
		
		JavaSparkContext sparkContext = SparkConfig.initSparkContext();
		
		JavaRDD<Double> javaRDD = sparkContext.parallelize(inputData);
		
		Double result = javaRDD.reduce((value1, value2) -> value1 + value2);
		
		System.out.println("Resultant Sum : " + result);
		
		sparkContext.close();
		
	}
}
