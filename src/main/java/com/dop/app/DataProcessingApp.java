package com.dop.app;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class DataProcessingApp {
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("AdvancedDataProcessingApp")
                .master("local[*]")
                .getOrCreate();

        // Sample data
        List<Person> data = Arrays.asList(
                new Person("Alice", 34, 3000.0),
                new Person("Bob", 45, 4000.0),
                new Person("Catherine", 29, 3500.0),
                new Person("David", 40, 5000.0),
                new Person("Alice", 35, 3200.0)  // Note: Duplicate name
        );

        // Create Dataset from the sample data
        Dataset<Person> ds = spark.createDataset(data, Encoders.bean(Person.class));

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

        // Collect metrics
        long rowCount = ds.count();
        System.out.println("Row count: " + rowCount);

        // Stop the Spark session
        spark.stop();
    }
}