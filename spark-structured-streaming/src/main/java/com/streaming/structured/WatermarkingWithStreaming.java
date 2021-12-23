package com.streaming.structured;

import java.util.Arrays;
import java.util.Collection;
import static org.apache.spark.sql.functions.*

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.streaming.config.SparkConfig;

public class WatermarkingWithStreaming {

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
		
		Dataset<Row>  results = sparkSession.sql("select cast(value as string) as course_name from viewing_figures");
		
		// Group the data by window and word and compute the count of each group
		Dataset<Row> windowedCounts = results
		    .withWatermark("timestamp", "10 minutes") // data arriving later than 10  minutes would be discarded by engine
		    .groupBy(
		        window(col("timestamp"), "10 minutes", "5 minutes"),
		        col("word"))
		    .count();
		
		results.show();
		

		sparkSession.close();

	}

}
