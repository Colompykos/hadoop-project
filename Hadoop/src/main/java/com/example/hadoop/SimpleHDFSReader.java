package com.example.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SimpleHDFSReader {
    // Mapper class that passes through the input key-value pairs
    public static class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Simply emit the input key and value
            context.write(key, value);
        }
    }

    // Reducer class that writes the key and all associated values
    public static class SimpleReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder allValues = new StringBuilder();
            for (Text value : values) {
                if (allValues.length() > 0) {
                    allValues.append(", ");
                }
                allValues.append(value.toString());
            }
            context.write(key, new Text(allValues.toString()));
        }
    }
}
