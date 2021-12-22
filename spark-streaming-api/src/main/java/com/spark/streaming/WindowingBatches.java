package com.spark.streaming;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.spark.config.SparkConfig;

import scala.Tuple2;

//prerequisite : Run the LoggingServer program as that is the streaming source for this operation
public class WindowingBatches {
	
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
		
		//Use a sliding window of 30 seconds : this will aggregate data in the last 30 seconds
		// windowDuration: width of the window; must be a multiple of this DStream's batching interval
		// slideDuration:  sliding interval of the window (i.e., the interval after which
		//                 the new DStream will generate RDDs); must be a multiple of this DStream's batching interval
		
		//generating word counts over the last 30 seconds of data, with default slideDuration set to batch size i.e 2 seconds 
		//pairDStream = pairDStream.reduceByKeyAndWindow((x, y) -> x + y, Durations.seconds(30));
		
		//generating word counts over the last 30 seconds of data, every 10 seconds. We would see results of last 30 seconds every 10seconds
		pairDStream = pairDStream.reduceByKeyAndWindow((x, y) -> x + y, Durations.seconds(30), Durations.seconds(10));
		
		//prints the data processed in last 2  seconds and keeps overwriting the previous RDD
		pairDStream.print();
		
		//Start receiving data and processing it using streamingContext.start().
		jssc.start();
		
		//Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
		jssc.awaitTermination();
		
	}

}
