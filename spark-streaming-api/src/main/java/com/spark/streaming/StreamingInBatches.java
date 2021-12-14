package com.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

//prerequisite : Run the LoggingServer program as that is the streaming source for this operation
public class StreamingInBatches {
	
	public static void main(String[] args) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.storage").setLevel(Level.ERROR);
		
		// java streaming context with batch size of 2seconds
		JavaStreamingContext jssc = SparkConfig.initSparkStreamingContext(2);
		
		//Define the input sources by creating input DStreams.
		JavaReceiverInputDStream<String> inputData = jssc.socketTextStream("localhost", 8989);
		
		//Define the streaming computations by applying transformation and output operations to DStreams.
		JavaDStream<String> results = inputData.map(item -> item);
		JavaPairDStream<String, Long> pairDStream = results.mapToPair(line -> new Tuple2<>(line.split(",")[0], 1L));
		pairDStream = pairDStream.reduceByKey((x, y) -> x + y);
		
		//prints the data processed in last 2  seconds and keeps overwriting the previous RDD
		pairDStream.print();
		
		//Start receiving data and processing it using streamingContext.start().
		jssc.start();
		
		//Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
		jssc.awaitTermination();
		
	}

}
