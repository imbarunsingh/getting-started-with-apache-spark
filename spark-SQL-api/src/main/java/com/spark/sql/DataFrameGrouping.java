package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import com.spark.config.SparkConfig;

public class DataFrameGrouping {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/biglog.txt");
		
//		Dataset<Row> groupedResult = sparkSession.sql("select level, date_format(datetime, 'MMMM') as Month, count(1) as total"
//				+ " from logging_table GROUP BY level, month order by cast(first(date_format(datetime, 'M')) as int), level");

		
		dataset = dataset.select(col("level"),
								date_format(col("datetime"), "MMM").alias("month"),
								date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
		
		dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
		
		dataset = dataset.orderBy(col("monthnum"), col("level")).drop(col("monthnum"));
		
		dataset.show(100);
		
		sparkSession.close();
		
	}

}
