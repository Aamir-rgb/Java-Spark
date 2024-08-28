package com.learningspark.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.when;
import org.apache.spark.sql.functions;




public class Application {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
     SparkSession spark = 
    		 new SparkSession.Builder().appName("CSV Reading Spark").master("local").getOrCreate();
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      ;
      Dataset<Row> df = spark.read().format("csv").option("header", true).load(classLoader.getResource("EmployeeData.csv").getFile());
      df.show();
      Dataset<Row> df1 = spark.read().format("csv").option("header", true).load(classLoader.getResource("Employee.csv").getFile());

      //Number Of Employees In Input File
      long rowCount = df.count();
      System.out.println(rowCount+"Row Count");

     
      //df.filter(df.col("salary") > 1500));
      //Average Salary Of Employee
      Dataset<Row> average_sal = df.agg(avg(col("salary")));
      average_sal.show();
       long count = average_sal.count();
      
      //Capture the 
      Row firstRow = average_sal.first();
      double averageSalary = firstRow.getDouble(0); // The column index 0 corresponds to the first column

      // Print the average salary
      System.out.println("Average Salary: " + averageSalary);
      
      //Total Bonus Amount Paid
      Dataset<Row> total_bonus = df.agg(sum(col("bonus")));
      Row first_row = total_bonus.first();
    //  int value_bonus = total_bonus.get
      total_bonus.show();
      System.out.println(first_row+"Bonus");
      
      //Maximum Salary 
      Dataset<Row> highest_sal = df.agg(max("salary"));
      highest_sal.show();
      Row first_row_sal = highest_sal.first();
      String highestSalary = first_row_sal.getString(0); // Use getDouble(0) if "salary" is of type Double
      Dataset<Row> highest_sal_employee = df.filter(col("salary").equalTo(highestSalary));
      highest_sal_employee.show();
      
      System.out.println(first_row_sal+"Maximum Salary");

    //How Many Employees Were Hired In 2020
      Dataset<Row> total_employee_year = df.withColumn("year", year(col("creationdate")));
      Dataset<Row> filteredDf = total_employee_year.filter(col("year").isNotNull().and(col("year").equalTo(2020)));
      System.out.println(filteredDf.count()+" Employees Hired In 2020");
      Dataset<Row> filtered_df = df.filter(col("salary").gt(30000));
      filtered_df.show();
      
       Dataset<Row> top5salaries = df.orderBy(df.col("salary").desc()).limit(5);
       
       Dataset<Row> female_employee = df.filter(df.col("gender").equalTo("Female"));
       female_employee.show();
       
       Dataset<Row> female_employee_salary = df.agg(sum(col("salary")));
       female_employee_salary.show();
       
       Dataset<Row> salary_df = df.filter(col("salary").geq(50000).and(col("salary").leq(75000)));
       
       Dataset<Row> bottom_salary_bonus = df.orderBy(df.col("bonus").asc()).limit(10);

       Dataset<Row> latest_creation_date = df.agg(max("creationdate"));
       Dataset<Row> earliest_creation_date = df.agg(min("creationdate"));
       Dataset<Row> salary_less = df.filter(col("salary").lt(50000));
       Dataset<Row> salary_greater_bonus = df.filter(col("salary").gt(col("bonus")));

       top5salaries.show();
       
       bottom_salary_bonus.show();
       
       latest_creation_date.show();

       earliest_creation_date.show();
       
       System.out.println(salary_less.count() + " employees havs salary less than 50000");

       salary_greater_bonus.show();
       
       System.out.println(salary_greater_bonus.count()+"Salary Greater than bonus ");
       
       Dataset<Row> groupedDf = df.groupBy("id")
    		    .agg(count("*").alias("count"));
       Dataset<Row> finalDf = groupedDf.filter(col("count").gt(1));
       
       finalDf.show();
       
       Dataset<Row> missing_gender = df.filter(col("Gender").isNull().or(col("Gender").equalTo("")));
       
       System.out.println(missing_gender.count()+"Missing Gender count");
       
       Dataset<Row> dfCleaned = df.withColumn(
               "bonus",
               when(col("bonus").isNull().or(col("bonus").equalTo("")), "100")
               .otherwise(col("bonus"))
           );

       dfCleaned.show();
       
       Dataset<Row> df_cleaned_sal = df.filter(col("salary").isNull().or(col("salary").lt(0)).or(col("salary").equalTo("")));
       Dataset<Row> df_cleaned_bonus = df.filter(col("bonus").isNull().or(col("bonus").lt(0)).or(col("bonus").equalTo("")));
       long countrow = df.count();
       long countsal = df_cleaned_sal.count();
       long countbonus = df_cleaned_bonus.count();
       System.out.println(countrow - (countsal+countbonus));
       
       Dataset<Row> filteredDF = df1.filter(functions.col("first_name").rlike("^J"));
       filteredDF.show();
       
       String pattern ="s$";
       Dataset<Row> endwithS = df1.filter(col("last_name").rlike(pattern));
       endwithS.show();

       
       Dataset<Row> longest_name_df = df.orderBy(functions.length(col("first_name")).desc()).limit(1);

       Row longestNameRow = longest_name_df.collectAsList().get(0);
       
       String longestName = longestNameRow.getAs("first_name");
       System.out.println("Longest Name: " + longestName.length());
       
       Dataset<Row> shortest_name_df = df.orderBy(functions.length(col("last_name")).asc()).limit(1);

       Row shortest_name = shortest_name_df.collectAsList().get(0);
       
       String shortest = shortest_name.getAs("last_name");
       System.out.println("Shortest Name: " + shortest.length());



       
	}

}
