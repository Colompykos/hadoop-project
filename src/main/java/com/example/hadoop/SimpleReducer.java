package com.example.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SimpleReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder concatenatedValues = new StringBuilder();
        for (Text value : values) {
            concatenatedValues.append(value.toString()).append(", ");
        }
        context.write(key, new Text(concatenatedValues.toString()));
    }
}
