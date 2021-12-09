
package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

import com.spark.config.SparkConfig;

public class AggregationDemo {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/exams/students.csv");
		
		//dataset.show(); //only showing top 20 rows
		
		System.out.println("There are total of : " + dataset.count() + " records");
		
		//one aggregation
		Dataset<Row> datasetOneAgg = dataset.groupBy("subject").agg(max("score").alias("Max Score"));
		datasetOneAgg.show();
		
		//more than one aggregation
		Dataset<Row> datasetMultipleAgg = dataset.groupBy("subject").agg(max("score").alias("Max Score"), min("score").alias("Min Score"));		
		datasetMultipleAgg.show();
		
		sparkSession.close();
		
	}

}
