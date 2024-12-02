package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HighestRatedTag {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String movieId = fields[1];
                String rating = fields[2];
                // Emit: movieId -> R:rating
                context.write(new Text(movieId), new Text("R:" + rating));
            }
        }
    }

    public static class TagMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String movieId = fields[1];
                String tag = fields[2];
                // Emit: movieId -> T:tag
                context.write(new Text(movieId), new Text("T:" + tag));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private Map<String, AggregateInfo> tagAggregates = new HashMap<>();

        private static class AggregateInfo {
            double totalRating = 0;
            int count = 0;

            void addRating(double rating) {
                totalRating += rating;
                count++;
            }

            double getAverage() {
                return count > 0 ? totalRating / count : 0;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> tags = new ArrayList<>();
            List<Double> ratings = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts.length == 2) {
                    if (parts[0].equals("T")) {
                        tags.add(parts[1]);
                    } else if (parts[0].equals("R")) {
                        ratings.add(Double.parseDouble(parts[1]));
                    }
                }
            }

            double avgRating = 0;
            if (!ratings.isEmpty()) {
                avgRating = ratings.stream().mapToDouble(r -> r).average().orElse(0);
            }

            for (String tag : tags) {
                tagAggregates.computeIfAbsent(tag, k -> new AggregateInfo())
                        .addRating(avgRating);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Find tag with highest average rating
            String bestTag = null;
            double highestAvg = Double.NEGATIVE_INFINITY;

            for (Map.Entry<String, AggregateInfo> entry : tagAggregates.entrySet()) {
                double avg = entry.getValue().getAverage();
                if (avg > highestAvg) {
                    highestAvg = avg;
                    bestTag = entry.getKey();
                }
            }

            if (bestTag != null) {
                context.write(new Text(bestTag), new DoubleWritable(highestAvg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highest rated tag");

        job.setJarByClass(HighestRatedTag.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TagMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
