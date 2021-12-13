package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

import com.spark.config.SparkConfig;

public class MultipleGroupingJavaAPI {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/biglog.txt");
		
		//java way of writing sql statements
		Dataset<Row> groupedResultUsingJavaAPI = dataset.select(col("level"),
					                                            date_format(col("datetime"), "MMMM").alias("month"),
					                                            date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
		
		groupedResultUsingJavaAPI = groupedResultUsingJavaAPI.groupBy("level", "month", "monthnum")
															.count().as("total")
															.orderBy("monthnum")
															.drop("monthnum");
		groupedResultUsingJavaAPI.show();
		
		sparkSession.close();
	}

}
