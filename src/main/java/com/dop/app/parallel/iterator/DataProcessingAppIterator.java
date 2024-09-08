package com.dop.app.parallel.iterator;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.spark.api.java.function.Function2;
import java.util.*;
import java.util.concurrent.*;

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


        Dataset<ProfileResult> resultDataset = data.mapPartitions(new ParallelProfilerFunction(), Encoders.bean(ProfileResult.class));

        // Use treeReduce with Function2
        ProfileResult result = resultDataset.javaRDD().treeReduce(new ProfileResultReducer());

        System.out.println(result);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000;
        System.out.println("Execution time: " + duration + " ms");

        spark.stop();
    }
}
class ProfileResultReducer implements Function2<ProfileResult, ProfileResult, ProfileResult>, Serializable {
    @Override
    public ProfileResult call(ProfileResult r1, ProfileResult r2) throws Exception {
        r1.merge(r2);
        return r1;
    }
}


class ParallelProfilerFunction implements MapPartitionsFunction<Row, ProfileResult>, Serializable {
    private static final int PARALLEL_THREADS = Runtime.getRuntime().availableProcessors();

    @Override
    public Iterator<ProfileResult> call(Iterator<Row> iterator) throws Exception {
        ProfileResult result = new ProfileResult();
        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);
        List<Future<?>> futures = new ArrayList<>();

        while (iterator.hasNext()) {
            List<Row> batch = new ArrayList<>();
            for (int i = 0; i < 1000 && iterator.hasNext(); i++) {
                batch.add(iterator.next());
            }

            futures.add(executor.submit(() -> processBatch(batch, result)));
        }

        for (Future<?> future : futures) {
            future.get(); // Wait for all tasks to complete
        }

        executor.shutdown();
        return Collections.singletonList(result).iterator();
    }

    private void processBatch(List<Row> batch, ProfileResult result) {
        for (Row row : batch) {
            result.processRow(row);
        }
    }
}

class ProfileResult implements Serializable {
    private StringProfiler nameProfiler = new StringProfiler();
    private DoubleProfiler ageProfiler = new DoubleProfiler();
    private DoubleProfiler salaryProfiler = new DoubleProfiler();

    public synchronized void processRow(Row row) {
        nameProfiler.processString(row.getString(0));
        ageProfiler.processDouble(row.getDouble(1));
        salaryProfiler.processDouble(row.getDouble(2));
    }

    public synchronized void merge(ProfileResult other) {
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