package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UserAverageRating {
    public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String userId = fields[0];
                double rating = Double.parseDouble(fields[2]);
                context.write(new Text(userId), new DoubleWritable(rating));
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "user average rating");
        job.setJarByClass(UserAverageRating.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
