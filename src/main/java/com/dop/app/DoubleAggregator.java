package com.dop.app;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class DoubleAggregator extends Aggregator<Double, DoubleAggregator.DoubleBuffer, String> {

    public static class DoubleBuffer implements Serializable {
        private double sum = 0.0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private int count = 0;

        public void add(double value) {
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        public double getSum() { return sum; }
        public double getMin() { return min; }
        public double getMax() { return max; }
        public int getCount() { return count; }
        public double getAverage() { return count > 0 ? sum / count : 0; }
    }

    @Override
    public DoubleBuffer zero() {
        return new DoubleBuffer();
    }

    @Override
    public DoubleBuffer reduce(DoubleBuffer buffer, Double value) {
        buffer.add(value);
        return buffer;
    }

    @Override
    public DoubleBuffer merge(DoubleBuffer b1, DoubleBuffer b2) {
        DoubleBuffer merged = new DoubleBuffer();
        merged.sum = b1.getSum() + b2.getSum();
        merged.min = Math.min(b1.getMin(), b2.getMin());
        merged.max = Math.max(b1.getMax(), b2.getMax());
        merged.count = b1.getCount() + b2.getCount();
        return merged;
    }

    @Override
    public String finish(DoubleBuffer reduction) {
        return String.format("Sum: %.2f | Avg: %.2f | Min: %.2f | Max: %.2f | Count: %d",
                reduction.getSum(),
                reduction.getAverage(),
                reduction.getMin(),
                reduction.getMax(),
                reduction.getCount());
    }

    @Override
    public Encoder<DoubleBuffer> bufferEncoder() {
        return Encoders.kryo(DoubleBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}