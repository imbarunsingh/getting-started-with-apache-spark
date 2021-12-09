package com.spark.core;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.spark.config.SparkConfig;

import scala.Tuple2;

public class TestingJoins {
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		JavaSparkContext sparkContext = SparkConfig.initSparkContext();
		
		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4, 18));
		visitsRaw.add(new Tuple2<>(6, 4));
		visitsRaw.add(new Tuple2<>(10, 9));
		
		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1, "Bob"));
		usersRaw.add(new Tuple2<>(4, "Sam"));
		usersRaw.add(new Tuple2<>(6, "Peter"));
		usersRaw.add(new Tuple2<>(19, "Ashley"));
		
		
		JavaPairRDD<Integer, Integer> visitsRDD = sparkContext.parallelizePairs(visitsRaw);
		
		JavaPairRDD<Integer, String> usersRDD = sparkContext.parallelizePairs(usersRaw);
		
		JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoinRDD = visitsRDD.join(usersRDD);
		
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinRDD = visitsRDD.leftOuterJoin(usersRDD);
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinRDD = visitsRDD.rightOuterJoin(usersRDD);
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinRDD = visitsRDD.fullOuterJoin(usersRDD);
		
		System.out.println("-------Inner Join--------");
		innerJoinRDD.foreach(x -> System.out.println(x));
		
		System.out.println("-------Left Outer Join--------");
		//leftJoinRDD.foreach(x -> System.out.println(x._2._2.orElse("null")));
		leftJoinRDD.foreach(x -> System.out.println(x));
		
		System.out.println("-------Right Outer Join--------");
		rightJoinRDD.foreach(x -> System.out.println(x));
		
		System.out.println("-------Full Outer Join--------");
		fullJoinRDD.foreach(x -> System.out.println(x));
		
		sparkContext.close();
		
		
	}

}
