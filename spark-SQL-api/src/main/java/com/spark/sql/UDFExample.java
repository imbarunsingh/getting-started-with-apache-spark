
package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import com.spark.config.SparkConfig;

public class UDFExample {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		//registering a function named : hasPassed : Lambda Way
		sparkSession.udf().register("hasPassed", (String grade, String subject) -> {
			if(subject.equals("Biology")) {
				return grade.contentEquals("A+");
			} else {
				return grade.contentEquals("C");
			}
		}, DataTypes.BooleanType);
		
		Dataset<Row> dataset = sparkSession
								.read()
								.option("header", true)
								.csv("src/main/resources/exams/students.csv");
		
		dataset.show(); //only showing top 20 rows
		
		//dataset = dataset.withColumn("pass", lit("YES")); // introduces a new column pass with value as YES
		//dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+"))); //lit function can have an expression as well
		
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject"))); // calling UDF function defined in line 26 with grade as method param for that method
		
		dataset.show();
		
		sparkSession.close();
		
	}

}
