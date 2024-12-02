package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// List movies for a genre = done
public class MoviesForGenre {
    public static class GenreFilterMapper extends Mapper<Object, Text, Text, Text> {
        private String searchGenre;

        @Override
        protected void setup(Context context) {
            // Get the genre to search for from configuration
            searchGenre = context.getConfiguration().get("searchGenre", "");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header row (movieId,title,genres)
            if (value.toString().startsWith("movieId")) {
                return;
            }

            try {
                // Split the CSV line, handling quoted values correctly
                String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                if (fields.length >= 3) {
                    String movieId = fields[0];
                    String title = fields[1];
                    String[] movieGenres = fields[2].split("\\|");  // Split multiple genres

                    // Check if any of the movie's genres match our search genre
                    for (String genre : movieGenres) {
                        if (genre.trim().equalsIgnoreCase(searchGenre.trim())) {
                            // Output format: key=genre, value=movieId,title
                            context.write(
                                    new Text(genre),
                                    new Text(movieId + "," + title)
                            );
                            break;  // Found a match, no need to check other genres
                        }
                    }
                }
            } catch (Exception e) {
                // Log malformed lines
                context.getCounter("Movies", "Malformed").increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MoviesForGenre <input_path> <output_path> <search_genre>");
            System.err.println("Example: MoviesForGenre movies.csv output Adventure");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("searchGenre", args[2]);  // Store the genre we're searching for

        Job job = Job.getInstance(conf, "movies for genre");
        job.setJarByClass(MoviesForGenre.class);
        job.setMapperClass(GenreFilterMapper.class);

        // We don't need a reducer for this task
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}