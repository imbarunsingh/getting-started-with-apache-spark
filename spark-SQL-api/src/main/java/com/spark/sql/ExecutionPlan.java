package com.spark.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.config.SparkConfig;

//Access Spark UI console on : http://localhost:4400

public class ExecutionPlan {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};
		
		StructType schema = new StructType(fields);
		
		Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, schema);
		
		//creates a a view like structure from teh dataset
		dataset.createOrReplaceTempView("my_logging_table");
		
		Dataset<Row> groupedResult = sparkSession.sql("select level, count(datetime) from my_logging_table"
													+ " group by level "
													+ "order by level");
		groupedResult.show();
		
		groupedResult.explain();
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Press Enter to close the spark session :: ");
		
		sc.nextLine();
		
		
		
		sparkSession.close();
	}

}
