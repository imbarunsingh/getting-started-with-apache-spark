package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

import com.spark.config.SparkConfig;

public class TempViewForSQL {
	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession sparkSession = SparkConfig.initSparkSession();

		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

		//dataset.show(); //only showing top 20 rows

		dataset.createOrReplaceTempView("my_students_table");

		Dataset<Row> results = sparkSession.sql("select * from my_students_table where subject='French'"); // can have
																											// any sql
		results.show();

		sparkSession.close();

	}

}
