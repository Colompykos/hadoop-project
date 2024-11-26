package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesForGenre {
    public static class GenreFilterMapper extends Mapper<Object, Text, Text, Text> {
        private String targetGenre;

        @Override
        protected void setup(Context context) {
            targetGenre = context.getConfiguration().get("genres");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("movieId")) return;

            String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length >= 3) {
                String title = fields[1];
                String[] genres = fields[2].split("\\|");
                for (String genre : genres) {
                    if (genre.equals(targetGenre)) {
                        context.write(new Text(genre), new Text(title));
                        break;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("genres", args[2]); // Pass genre as argument
        Job job = Job.getInstance(conf, "movies for genre");
        job.setJarByClass(MoviesForGenre.class);
        job.setMapperClass(GenreFilterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}