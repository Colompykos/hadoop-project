package com.example.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;

public class HighestRatedMovie {
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            int userId = Integer.parseInt(fields[0]);
            String movieId = fields[1];
            String rating = fields[2];
            context.write(new IntWritable(userId), new Text(movieId + ":" + rating));
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String highestRatedMovie = null;
            double maxRating = Double.MIN_VALUE;

            for (Text value : values) {
                String[] movieRating = value.toString().split(":");
                double rating = Double.parseDouble(movieRating[1]);
                if (rating > maxRating) {
                    maxRating = rating;
                    highestRatedMovie = movieRating[0];
                }
            }
            context.write(key, new IntWritable(Integer.parseInt(highestRatedMovie)));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HighestRatedMovie.class);
        job.setJobName("Highest Rated Movie");
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}