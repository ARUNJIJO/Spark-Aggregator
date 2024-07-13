package com.dop.app;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class StringAggregator extends Aggregator<String, StringAggregator.StringBuffer, String> {

    public static class StringBuffer implements Serializable {
        private StringBuilder concatenated = new StringBuilder();
        private Set<String> unique = new HashSet<>();
        private int count = 0;

        public void add(String value) {
            if (concatenated.length() > 0) {
                concatenated.append(", ");
            }
            concatenated.append(value);
            unique.add(value);
            count++;
        }

        public String getConcatenated() {
            return concatenated.toString();
        }

        public Set<String> getUnique() {
            return unique;
        }

        public int getCount() {
            return count;
        }
    }

    @Override
    public StringBuffer zero() {
        return new StringBuffer();
    }

    @Override
    public StringBuffer reduce(StringBuffer buffer, String value) {
        buffer.add(value);
        return buffer;
    }

    @Override
    public StringBuffer merge(StringBuffer b1, StringBuffer b2) {
        StringBuffer merged = new StringBuffer();
        merged.concatenated = new StringBuilder(b1.getConcatenated());
        if (merged.concatenated.length() > 0) {
            merged.concatenated.append(", ");
        }
        merged.concatenated.append(b2.getConcatenated());
        merged.unique.addAll(b1.getUnique());
        merged.unique.addAll(b2.getUnique());
        merged.count = b1.getCount() + b2.getCount();
        return merged;
    }

    @Override
    public String finish(StringBuffer reduction) {
        return String.format("Concatenated: %s | Unique Count: %d | Total Count: %d",
                reduction.getConcatenated(),
                reduction.getUnique().size(),
                reduction.getCount());
    }

    @Override
    public Encoder<StringBuffer> bufferEncoder() {
        return Encoders.kryo(StringBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}