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

public class OutputSinkDemo {

	public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.storage").setLevel(Level.WARN);

		// java streaming context with batch size of 2seconds
		SparkSession sparkSession = SparkConfig.initSparkSession();

		String topic = "view-records";
		
		Dataset<Row> dataset = sparkSession.readStream()
										  .format("kafka")
										  .option("kafka.bootstrap.servers", "localhost:9092")
										  .option("subscribe", topic)
										  .load();
		
		dataset.createOrReplaceTempView("viewing_figures");
		
		Dataset<Row>  results = sparkSession.sql("select cast(value as string) as course_name from viewing_figures");
		
		StreamingQuery query = results.writeStream()
									  .format("console")
									  .outputMode(OutputMode.Append())
									  .start();
		
		query.awaitTermination();
	}

}
