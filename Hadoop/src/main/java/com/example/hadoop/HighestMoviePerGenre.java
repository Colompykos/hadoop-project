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

//Done
public class HighestMoviePerGenre {

    // Mapper for movies.csv
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("movieId")) return;

            String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length >= 3) {
                String movieId = fields[0];
                String title = fields[1];
                String[] genres = fields[2].split("\\|");

                for (String genre : genres) {
                    // Emit: genre -> M:movieId,title
                    context.write(new Text(genre), new Text("M:" + movieId + "," + title));
                }
            }
        }
    }

    // Mapper for ratings.csv
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Double> movieRatings = new HashMap<>();
        private Map<String, Integer> movieCounts = new HashMap<>();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("userId")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String movieId = fields[1];
                double rating = Double.parseDouble(fields[2]);

                // Accumulate ratings
                movieRatings.merge(movieId, rating, Double::sum);
                movieCounts.merge(movieId, 1, Integer::sum);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            // Emit average ratings for each movie
            for (Map.Entry<String, Double> entry : movieRatings.entrySet()) {
                String movieId = entry.getKey();
                double avgRating = entry.getValue() / movieCounts.get(movieId);
                // Emit: movieId -> R:avgRating
                context.write(new Text(movieId), new Text("R:" + avgRating));
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, MovieInfo> bestMovies = new HashMap<>();

        private static class MovieInfo {
            String title;
            double rating;

            MovieInfo(String title, double rating) {
                this.title = title;
                this.rating = rating;
            }
        }

        @Override
        protected void reduce(Text genre, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, String> movieTitles = new HashMap<>();
            Map<String, Double> movieRatings = new HashMap<>();

            // Process all values
            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts.length == 2) {
                    if (parts[0].equals("M")) {
                        String[] movieData = parts[1].split(",", 2);
                        movieTitles.put(movieData[0], movieData[1]);
                    } else if (parts[0].equals("R")) {
                        String movieId = parts[1];
                        double rating = Double.parseDouble(parts[1]);
                        movieRatings.put(movieId, rating);
                    }
                }
            }

            // Find highest rated movie for this genre
            String bestMovieId = null;
            double highestRating = Double.NEGATIVE_INFINITY;

            for (Map.Entry<String, Double> entry : movieRatings.entrySet()) {
                if (entry.getValue() > highestRating && movieTitles.containsKey(entry.getKey())) {
                    highestRating = entry.getValue();
                    bestMovieId = entry.getKey();
                }
            }

            if (bestMovieId != null) {
                String title = movieTitles.get(bestMovieId);
                context.write(genre,
                        new Text(String.format("%s\t%.2f", title, highestRating)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highest movie per genre");

        job.setJarByClass(HighestMoviePerGenre.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
