package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

public class HighestRatedMovieWithTitle {

    // Mapper for ratings.csv
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 4) {
                String userId = fields[0];
                String movieId = fields[1];
                String rating = fields[2];

                // Emit: movieId -> R:userId,rating
                outKey.set(movieId);
                outValue.set("R:" + userId + "," + rating);
                context.write(outKey, outValue);
            }
        }
    }

    // Mapper for movies.csv
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("movieId")) return;

            String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length >= 2) {
                String movieId = fields[0];
                String title = fields[1];

                // Emit: movieId -> M:title
                outKey.set(movieId);
                outValue.set("M:" + title);
                context.write(outKey, outValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String movieTitle = null;
            Map<String, Double> userRatings = new HashMap<>();

            // Process all values
            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts.length == 2) {
                    if (parts[0].equals("M")) {
                        movieTitle = parts[1];
                    } else if (parts[0].equals("R")) {
                        String[] ratingData = parts[1].split(",");
                        String userId = ratingData[0];
                        double rating = Double.parseDouble(ratingData[1]);

                        // Update highest rating for this user
                        userRatings.compute(userId, (k, v) ->
                                v == null ? rating : Math.max(v, rating));
                    }
                }
            }

            // Only emit if we have both movie title and ratings
            if (movieTitle != null && !userRatings.isEmpty()) {
                for (Map.Entry<String, Double> entry : userRatings.entrySet()) {
                    String userId = entry.getKey();
                    double rating = entry.getValue();
                    // Emit: userId -> movieTitle,rating
                    context.write(
                            new Text(userId),
                            new Text(String.format("%s\t%.1f", movieTitle, rating))
                    );
                }
            }
        }
    }

    // Final reducer to get highest rated movie per user
    public static class UserHighestReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text userId, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String bestMovie = null;
            double highestRating = Double.NEGATIVE_INFINITY;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                double rating = Double.parseDouble(parts[1]);
                if (rating > highestRating) {
                    highestRating = rating;
                    bestMovie = parts[0];
                }
            }

            if (bestMovie != null) {
                context.write(userId, new Text(String.format("%s\t%.1f", bestMovie, highestRating)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highest rated movie with title");

        job.setJarByClass(HighestRatedMovieWithTitle.class);

        // Configure multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        job.setReducerClass(UserHighestReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
