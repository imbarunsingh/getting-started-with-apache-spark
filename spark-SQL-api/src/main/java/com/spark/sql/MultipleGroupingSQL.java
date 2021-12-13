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

public class MultipleGroupingSQL {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/biglog.txt");
		
		dataset.createOrReplaceTempView("logging_table");
		// Refer for more on date formating and Spark SQL
		//https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
		// aggregate function has to be used only on column that is not part of group by clause or can use count(1) 
		Dataset<Row> groupedResultUsingSQL = sparkSession.sql("select level, date_format(datetime, 'MMMM') as Month, count(1) as total, first(date_format(datetime, 'M')) as monthnum"
															+ " from logging_table"
															+ " GROUP BY level, month" 
															+ " order by cast(monthnum as int),level");
		groupedResultUsingSQL.show();
		
		sparkSession.close();
	}

}
