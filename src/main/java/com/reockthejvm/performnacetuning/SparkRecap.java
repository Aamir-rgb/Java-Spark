package com.reockthejvm.performnacetuning;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.Function;


public class SparkRecap {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = new SparkSession.Builder().appName("Spark Optimization").master("local").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Integer> numbers = IntStream.rangeClosed(1, 100000)
                .boxed()
                .collect(Collectors.toList());
        
        JavaRDD<Integer> rdd1 = sc.parallelize(numbers);
        JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer x) {
                return x * 2;
            }
        });
        JavaRDD<Integer> rdd3 = rdd2.repartition(23);

        List<Integer> firstTenElements = rdd1.take(10);
        System.out.println("First 10 elements: " + firstTenElements);
        System.out.println(rdd2.count()+"count");
        sc.stop();

	}

}
