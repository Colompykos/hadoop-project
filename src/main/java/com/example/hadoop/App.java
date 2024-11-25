package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: SimpleHDFSReader <input path> <output path>");
            System.exit(-1);
        }

        // Create configuration
        Configuration conf = new Configuration();

        // Create job
        Job job = Job.getInstance(conf, "Simple HDFS Reader");
        job.setJarByClass(SimpleHDFSReader.class);

        // Setup MapReduce job
        job.setMapperClass(SimpleHDFSReader.SimpleMapper.class);
        job.setReducerClass(SimpleHDFSReader.SimpleReducer.class);

        // Specify key / value types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    }

