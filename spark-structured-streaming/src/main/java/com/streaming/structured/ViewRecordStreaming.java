package com.streaming.structured;

import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.streaming.config.SparkConfig;

public class ViewRecordStreaming {

	public static void main(String[] args) throws InterruptedException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.storage").setLevel(Level.WARN);

		// java streaming context with batch size of 2seconds
		SparkSession sparkSession = SparkConfig.initSparkSession();

		Collection<String> topics = Arrays.asList("view-records");
		
		Dataset<Row> dataset = sparkSession.read()
										  .format("kafka")
										  .option("kafka.bootstrap.servers", "localhost:9092")
										  .option("subscribe", "view-records")
										  .load();
		
		dataset.createOrReplaceTempView("viewing_figures");
		
		Dataset<Row>  results = sparkSession.sql("select value from viewing_figures");
		results.show();
		

		sparkSession.close();

	}

}
