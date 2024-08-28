package com.learningspark.part2structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

import java.util.concurrent.TimeoutException;

public class StreamingDataFrames {

	static SparkSession spark;
	Dataset<Row> lines;
	StreamingQuery query;
	Dataset<Row> shortlines;

	public static void main(String[] args) throws StreamingQueryException, TimeoutException {
		// TODO Auto-generated method stub
		spark = new SparkSession.Builder().appName("Spark Strutcured Streaming").master("local[*]").getOrCreate();
		
		StructType stockSchema = new StructType(new StructField[]{
                DataTypes.createStructField("symbol", DataTypes.StringType, true),
                DataTypes.createStructField("date", DataTypes.DateType, true), // Use DateType for date
                DataTypes.createStructField("price", DataTypes.DoubleType, true)
        });
		Dataset<Row> stocksDs = spark.readStream()
                .format("csv")
                .option("header", "true")
                .option("dateFormat", "MMM d yyyy")
                .schema(stockSchema)
                .load("src/main/resources/data/stocks");
		 StreamingQuery query = stocksDs.writeStream()
	                .format("console")
	                .outputMode("append")
	                .trigger(Trigger.ProcessingTime("2 seconds"))
	                .start();
		query.awaitTermination();
		//new StreamingDataFrames().readFromSocket();
	}

	public void readFromSocket() throws StreamingQueryException, TimeoutException {
//		Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 12345)
//				.load();
//
//// Filter lines where the length of the value is less than or equal to 5
//		Dataset<Row> shortlines = lines.filter(length(col("value")).leq(5));
//
//// Write the filtered output to the console
//		query = shortlines.writeStream().format("console").outputMode("append").start();
//
//// Wait for the streaming to finish
//		query.awaitTermination();

	}

}
