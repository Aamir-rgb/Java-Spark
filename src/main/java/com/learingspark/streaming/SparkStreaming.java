package com.learingspark.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

public class SparkStreaming {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = new SparkSession.Builder().appName("Spark Streaming").master("local[2]")
				.getOrCreate();
        Dataset<Row>	carsDs = spark.read().format("json").option("inferSchema", "true").load("src/main/resources/data/cars.json");
        carsDs.show();
         
        Dataset<Row> usefulData = carsDs.select(
        	    col("Name"),
        	    col("Year"),
        	    (col("Weight_in_lbs").divide(2.2)).as("Weight_in_kg"),
        	    col("Weight_in_lbs").divide(2.2).as("Weight_in_kg_2.2")
        	);
        
        Dataset<Row> carsWeight = usefulData.select(col("Weight_in_kg"));
        carsWeight.show();
        
        Dataset<Row> Europeancars = usefulData.where(col("Origin").notEqual("USA"));
        Europeancars.show();
        
        Dataset<Row> averageHp = carsDs.select(avg(col("Horsepower"))).as("average_hp");
        averageHp.show();
        
        RelationalGroupedDataset countByOrigin = carsDs.groupBy(col("Origin"));
        Dataset<Row> countResult = countByOrigin.count(); // Perform the count aggregation

        // Show the result in the console
        countResult.show();
        
        Dataset<Row> guitarPlayers = spark.read().format("json").option("inferSchema", "true").load("src/main/resources/data/guitarPlayers.json");

        Dataset<Row> bands = spark.read().format("json").option("inferSchema", "true").load("src/main/resources/data/bands.json");
        bands.show();

        Dataset<Row> guitaristBand = guitarPlayers.join(bands,guitarPlayers.col("band").equalTo(bands.col("id")));
        guitaristBand.show();
        
        carsDs.createOrReplaceTempView("cars");
        
        Dataset<Row> carsSql = spark.sql("SELECT * FROM cars");

        carsSql.show();
        

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        
        List<Integer> numbersList = IntStream.rangeClosed(1, 1000000)
                .boxed()
                .collect(Collectors.toList());
        
        List<Integer> doubleNumber = (List<Integer>) ((IntStream) numbersList).map(x -> x*2);
        
        RDD<Row> carsRDD = carsDs.rdd();
        RDD<Row> guitarPlayersRDD = guitarPlayers.rdd();
        }

}
