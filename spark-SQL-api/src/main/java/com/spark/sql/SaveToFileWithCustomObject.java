package com.spark.sql;

import static org.apache.spark.sql.functions.col;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.spark.config.SparkConfig;
import com.spark.model.Person;

public class SaveToFileWithCustomObject {
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		String outputPath = "src/main/resources/person".concat(LocalDate.now().toString());
		
		SparkSession sparkSession = SparkConfig.initSparkSession();
		
		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge("32");
		
		List<Person> personList = new ArrayList<>();
		personList.add(person);
		//personList.add(null);
		personList.add(new Person());

		
		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		
		// StringUtils.isNotBlank(value.getName())
		
		Dataset<Person> javaBeanDS = sparkSession.createDataset(personList, personEncoder);
						
		Dataset<Person> filtered = javaBeanDS.filter(javaBeanDS.col("age").isNotNull());
		
		filtered.show();
		
							
		filtered.coalesce(1)
				.write()
				.format("com.databricks.spark.csv")
				.option("header", true)
				.option("delimiter", "|")
				.save(outputPath);
		
		filtered.coalesce(1)
		.write()		
		.text(outputPath);
		
		sparkSession.close();
	}

}
