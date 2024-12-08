package com.example.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieJoin {
    public static class UserMovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 2) {
                int userId = Integer.parseInt(fields[0]);
                int movieId = Integer.parseInt(fields[1]);
                context.write(new IntWritable(movieId), new Text("USER:" + userId));
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] fields = value.toString().split(",");
            int movieId = Integer.parseInt(fields[0]);
            String movieName = fields[1];
            context.write(new IntWritable(movieId), new Text("MOVIE:" + movieName));
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable movieId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieName = null;
            List<Integer> userIds = new ArrayList<>();

            for (Text value : values) {
                String str = value.toString();
                if (str.startsWith("MOVIE:")) {
                    movieName = str.substring(6);
                } else if (str.startsWith("USER:")) {
                    userIds.add(Integer.parseInt(str.substring(5)));
                }
            }

            if (movieName != null && !userIds.isEmpty()) {
                for (Integer userId : userIds) {
                    context.write(new IntWritable(userId), new Text(movieName));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(MovieJoin.class);
        job.setJobName("Movie Join");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}