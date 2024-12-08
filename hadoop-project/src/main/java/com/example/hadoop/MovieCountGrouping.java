package com.example.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class MovieCountGrouping {
    public static class MovieCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 2) {
                String movieName = fields[1].trim();
                context.write(new Text(movieName), one);
            }
        }
    }

    public static class MovieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class GroupingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 2) {
                try {
                    String movieName = fields[0].trim();
                    int count = Integer.parseInt(fields[1].trim());
                    context.write(new IntWritable(count), new Text(movieName));
                } catch (NumberFormatException e) {
                    System.err.println("Malformed record: " + value.toString());
                }
            } else {
                System.err.println("Insufficient fields: " + value.toString());
            }
        }
    }

    public static class GroupingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder movieList = new StringBuilder();
            for (Text movie : values) {
                if (movieList.length() > 0) {
                    movieList.append(" ");
                }
                movieList.append(movie.toString());
            }
            context.write(key, new Text(movieList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJarByClass(MovieCountGrouping.class);
        job1.setJobName("Movie Count");

        job1.setMapperClass(MovieCountMapper.class);
        job1.setReducerClass(MovieCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_temp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance();
        job2.setJarByClass(MovieCountGrouping.class);
        job2.setJobName("Group by Count");
        job2.setMapperClass(GroupingMapper.class);
        job2.setReducerClass(GroupingReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "_temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}