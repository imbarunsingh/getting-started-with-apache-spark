package com.spark.core;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

public class KeywordRankingExample {
	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		JavaSparkContext sparkContext = SparkConfig.initSparkContext();

		sparkContext.textFile("src/main/resources/subtitles/input.txt")
					.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()) //letters only RDD : remove anything that is not a word with regex
					.filter(lettersOnlyRDD -> lettersOnlyRDD.trim().length() > 0)  // removed blank lines
					.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator()) // split by space and get flattened map of words
					.filter(word -> !Util.isBoring(word) && StringUtils.isNotBlank(word)) // filtering not boring words and not blank string
					.mapToPair(word -> new Tuple2<String, Long>(word, 1L)) // create a key value pair(key : word, value: count)
					.reduceByKey((value1, value2) -> value1 + value2) // groups words count
					.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1)) // so as we can sort by key
					.sortByKey(false)
					.take(10)
					.forEach(value -> System.out.println(value));
		
		//Code hack to access Spark Web UI on localhost:4040
		Scanner sc = new Scanner(System.in);
		sc.nextLine();

		sparkContext.close();

	}

}
