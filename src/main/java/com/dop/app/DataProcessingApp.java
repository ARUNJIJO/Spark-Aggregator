package com.dop.app;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class DataProcessingApp {
    public static void main(String[] args) {


        String csvFile = "/Users/befyd/IdeaProjects/Demo/src/main/resources/data"; // Replace with your CSV file path

        SparkSession spark = SparkSession.builder()
                .appName("DataProcessingAppIterator")
                .master("local[*]")
                .getOrCreate();

        long startTime = System.nanoTime();

        Dataset<Row> ds = spark.read()
                .option("header", "true")
                .csv(csvFile);



        // Project the dataset to the specific columns
        Dataset<Row> projectedDs = ds.selectExpr("name", "CAST(age AS DOUBLE) AS age", "salary");

        // Define the advanced aggregators
        StringAggregator stringAgg = new StringAggregator();
        DoubleAggregator doubleAgg = new DoubleAggregator();

        // Register the UDFs
        UserDefinedFunction stringAggUdf = functions.udaf(stringAgg, Encoders.STRING());
        UserDefinedFunction doubleAggUdf = functions.udaf(doubleAgg, Encoders.DOUBLE());

        // Perform the aggregations
        Dataset<Row> result = projectedDs.agg(
                stringAggUdf.apply(functions.col("name")).as("name_agg"),
                doubleAggUdf.apply(functions.col("age")).as("age_agg"),
                doubleAggUdf.apply(functions.col("salary")).as("salary_agg")
        );

        // Show the aggregated results
        result.show(false);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000;  // Convert to milliseconds

        System.out.println("Execution time: " + duration + " ms");


        // Stop the Spark session
        System.out.println(ds.count() + "count is ");

    }
}