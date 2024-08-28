package com.learningspark.part2structuredstreaming;

import org.apache.spark.sql.SparkSession;

public class StreamingAggregation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = new SparkSession.Builder().appName("Spark Strutcured Streaming").master("local[*]").getOrCreate();

	}

}
