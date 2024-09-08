package com.dop.app.iterator;

import com.dop.app.Person;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.*;

public class DataProcessingAppIterator {
    public static void main(String[] args) {
        String csvFile = "/Users/befyd/IdeaProjects/Demo/src/main/resources/data"; // Replace with your CSV file path

        SparkSession spark = SparkSession.builder()
                .appName("DataProcessingAppIterator")
                .master("local[*]")
                .getOrCreate();

        long startTime = System.nanoTime();

        Dataset<Row> tmp = spark.read()
                .option("header", "true")
                .csv(csvFile);


        // Project the dataset to the specific columns
        Dataset<Row> data = tmp.selectExpr("name", "CAST(age AS DOUBLE) AS age", "cast(salary as double) as salary ");


        Dataset<ProfileResult> resultDataset = data.mapPartitions(new ProfilerFunction(), Encoders.bean(ProfileResult.class));

        ProfileResult result = resultDataset.reduce(new ProfileResultReducer());

        System.out.println(result);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000;  // Convert to milliseconds

        System.out.println("Execution time: " + duration + " ms");

        spark.stop();
    }
}

class ProfileResultReducer implements ReduceFunction<ProfileResult>, Serializable {
    @Override
    public ProfileResult call(ProfileResult r1, ProfileResult r2) throws Exception {
        r1.merge(r2);
        return r1;
    }
}

class ProfilerFunction implements MapPartitionsFunction<Row, ProfileResult>, Serializable {
    @Override
    public Iterator<ProfileResult> call(Iterator<Row> iterator) throws Exception {
        ProfileResult result = new ProfileResult();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            result.processRow(row);
        }
        return java.util.Collections.singletonList(result).iterator();
    }
}

class ProfileResult implements Serializable {
    private StringProfiler nameProfiler = new StringProfiler();
    private DoubleProfiler ageProfiler = new DoubleProfiler();
    private DoubleProfiler salaryProfiler = new DoubleProfiler();

    public void processRow(Row row) {
        nameProfiler.processString(row.getString(0));
        ageProfiler.processDouble(row.getDouble(1));
        salaryProfiler.processDouble(row.getDouble(2));
    }

    public void merge(ProfileResult other) {
        nameProfiler.merge(other.nameProfiler);
        ageProfiler.merge(other.ageProfiler);
        salaryProfiler.merge(other.salaryProfiler);
    }

    @Override
    public String toString() {
        return "Name aggregation: " + nameProfiler + "\n" +
                "Age aggregation: " + ageProfiler + "\n" +
                "Salary aggregation: " + salaryProfiler;
    }
}

class StringProfiler implements Serializable {
    private String concatenated = "";
    private Set<String> unique = new HashSet<>();
    private int count = 0;

    public void processString(String value) {
        if (!concatenated.isEmpty()) {
            concatenated += ", ";
        }
        concatenated += value;
        unique.add(value);
        count++;
    }

    public void merge(StringProfiler other) {
        if (!concatenated.isEmpty() && !other.concatenated.isEmpty()) {
            concatenated += ", ";
        }
        concatenated += other.concatenated;
        unique.addAll(other.unique);
        count += other.count;
    }

    @Override
    public String toString() {
        return String.format("Concatenated: %s | Unique Count: %d | Total Count: %d",
                concatenated, unique.size(), count);
    }
}

class DoubleProfiler implements Serializable {
    private double sum = 0.0;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private int count = 0;

    public void processDouble(double value) {
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        count++;
    }

    public void merge(DoubleProfiler other) {
        sum += other.sum;
        min = Math.min(min, other.min);
        max = Math.max(max, other.max);
        count += other.count;
    }

    @Override
    public String toString() {
        return String.format("Sum: %.2f | Avg: %.2f | Min: %.2f | Max: %.2f | Count: %d",
                sum, count > 0 ? sum / count : 0, min, max, count);
    }
}