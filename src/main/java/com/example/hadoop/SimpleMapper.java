package com.example.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SimpleMapper extends Mapper<Object, Text, Text, Text> {
    private Text lineKey = new Text();
    private Text lineValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Key is the line number, Value is the line content
        lineKey.set(key.toString());
        lineValue.set(value.toString());
        context.write(lineKey, lineValue);
    }
}