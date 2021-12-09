
package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import com.spark.config.SparkConfig;

public class DatasetApiDemo {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/exams/students.csv");
		
		dataset.show(); //only showing top 20 rows
		
		System.out.println("There are total of : " + dataset.count() + " records");
		
		Row firstRow = dataset.first(); // gets the first row  of record
		
		String subject = firstRow.getAs("subject").toString();
		System.out.println("Subject :: " + subject);
		
		int year = Integer.parseInt(firstRow.getAs("year"));
		System.out.println("Year :: " + year);
		
		
		Dataset<Row> stringFilteredResults = dataset.filter("subject = 'Modern Art' AND score >= 50 ");
		stringFilteredResults.show();
		
//		Dataset<Row> lambdaFilteredResults = dataset.filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("score")) > 50);
//		lambdaFilteredResults.show();
		
		Dataset<Row> columnFilteredResults = dataset.filter(col("subject").equalTo("Modern Art").and(col("score").$greater(70)));
		columnFilteredResults.show();
		
		sparkSession.close();
		
	}

}
