package com.streaming.structured;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.streaming.config.SparkConfig;

public class WindowStreamingDemo {

	public static void main(String[] args) throws StreamingQueryException, TimeoutException	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

		SparkSession session = SparkConfig.initSparkSession();
		
		session.conf().set("spark.sql.shuffle.partitions", "10");
		
		Dataset<Row> df = session.readStream()
				                 .format("kafka")
				                 .option("kafka.bootstrap.servers", "localhost:9092")
				                 .option("subscribe", "view-records")
				                 .load();
		
		// start some dataframe operations
		df.createOrReplaceTempView("viewing_figures");
		
		// key, value, timestamp
		//NOTE : the vent time is not when teh records are processed but when they arrived on Kafka
		Dataset<Row> results = session.sql("select window,"
											+ " cast (value as string) as course_name,"
											+ " sum(1) as watched_count"
											+ " from viewing_figures"
											+ " group by window(timestamp,'2 minutes'),course_name"); //window of 2 minutes
				
		StreamingQuery query = results
							   .writeStream()
							   .format("console")
							   .outputMode(OutputMode.Update())
							   .option("truncate", false)
							   .option("numRows", 50)
							   .start();
		
		query.awaitTermination();		
	}

}
