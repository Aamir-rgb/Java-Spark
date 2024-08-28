package com.learningspark.joins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.broadcast;


public class SparkJoins {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = new SparkSession.Builder().appName("Spark Optimization").master("local").getOrCreate();

		Dataset<Row> guitarsDs = spark.read().option("inferSchema","True").json("src/main./resources/data/guitars.json");
		guitarsDs.show();
		
		Dataset<Row> guitarPlayersDs = spark.read().option("inferSchema","True").json("src/main/resources/data/guitarPlayers.json");

		Dataset<Row> bandsDs = spark.read().option("inferSchema","True").json("src/main./resources/data/bands.json");

		Dataset<Row> guitaristbandsDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"inner");
		
		
		Dataset<Row> guitaristbandsLeftOuterDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"left_outer");

		
		Dataset<Row> guitaristbandRightOuterDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"right_outer");

		
		Dataset<Row> guitaristbandOuterDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"outer");

		
		Dataset<Row> guitaristbandLeftSemiDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"left_semi");

		
		Dataset<Row> guitaristbandLeftAntiDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"left_anti");

//		guitaristbandsDs.explain();
//		
//		guitaristbandsLeftOuterDs.show(30);
//		
//		guitaristbandRightOuterDs.show();
		
		guitaristbandLeftAntiDs.show();
		
		
	//	guitaristbandLeftSemiDs.show();
		
//		Dataset<Row> joinedDs = guitarPlayersDs
//			    .join(
//			        guitarsDs.withColumnRenamed("id", "bandid"), // Renaming "id" to "bandid" in guitarsDs
//			        expr("array_contains(guitarPlayersDs.bands, bandid)") // Checking if bandid is in the bands array
//			    );		// Checking if bandid is in the bands array
		//joinedDs.show();
		
//		Dataset<Row> broadcastJoinedDs = guitarPlayersDs.join(bandsDs,guitarPlayersDs.col("band").equalTo(bandsDs.col("id")),"outer");
//
//
//			    .join(
//			        broadcast(guitarsDs.withColumnRenamed("id", "bandid")), // Broadcast the small dataset
//			        expr("array_contains(guitarPlayersDs.bands, bandid)") // Join condition
//			    );
			    
			    
			    Dataset<Row> broadcastJoinedDs = guitarPlayersDs
			    	    .join(broadcast(bandsDs), guitarPlayersDs.col("band").equalTo(bandsDs.col("id")), "outer");
			    
			    broadcastJoinedDs.show();
	}

}
