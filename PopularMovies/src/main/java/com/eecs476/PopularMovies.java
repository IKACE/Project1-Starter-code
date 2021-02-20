package com.eecs476;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PopularMovies {
  //TODO Finish Part1 Q2 here
  private final static IntWritable one = new IntWritable(1);
//  private static Map<String, ArrayList<String>> ID2Genres = new HashMap<>();
//  private static Map<String, String> ID2Names = new HashMap<>();
  private static Map<Integer, HashMap<String, Integer>> GenresCounter = new HashMap<>();


//  // movie id to genres
//  public static class ID2GenresMapper
//          extends Mapper<LongWritable, Text, Text, IntWritable>{
//
//    // Output:
//    // We have the hardcoded value in our case which is 1: IntWritable
//    // The key is the tokenized words: Text
//    private Text word = new Text();
//
//    // Input:
//    // We define the data types of input and output key/value pair after the class declaration using angle brackets.
//    // Both the input and output of the Mapper is a key/value pair.
//
//    // The key is nothing but the offset of each line in the text file: LongWritable
//    // The value is each individual line (as shown in the figure at the right): Text
//    public void map(LongWritable key, Text value, Context context
//    ) throws IOException, InterruptedException {
//      String[] strs = value.toString().split(",");
//      ID2Genres.put(strs[0], new ArrayList<>());
//      for (int i=1; i<strs.length; i++) {
//        ID2Genres.get(strs[0]).add(strs[i]);
//      }
//    }
//    // We have written a java code where we have tokenized each word
//    // and assigned them a hardcoded value equal to 1.
//    // Eg: Dear 1, Bear 1,
//  }
//
//  public static class ID2GenresReducer
//          extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//  }

  // movideID -> name
//  public static class ID2NamesMapper
//          extends Mapper<LongWritable, Text, Text, IntWritable> {
//    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//      String[] strs = value.toString().split(",", 2);
//      ID2Names.put(strs[0], strs[1]);
//    }
//  }
//
//  public static class ID2NamesReducer
//          extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//  }

  public static class CounterMapper
          extends Mapper<LongWritable, Text, Text, Text>{
    private static Map<String, ArrayList<String>> ID2Genres = new HashMap<>();
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String genresFileName = conf.get("genresFile");
      Path newPath = new Path(genresFileName);
      FileSystem fs = newPath.getFileSystem(conf);
      FSDataInputStream inputStream = fs.open(newPath);
      BufferedReader bufferedReader = new BufferedReader(
              new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      String line = new String();
      while ((line = bufferedReader.readLine()) != null) {
        String[] strs = line.toString().split(",");
        ID2Genres.put(strs[0], new ArrayList<>());
        for (int i=1; i<strs.length; i++) {
          ID2Genres.get(strs[0]).add(strs[i]);
        }
      }
//      System.out.println("ID2Genres: " + ID2Genres);
    }

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] strs = value.toString().split(",");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(Long.parseLong(strs[3]) * 1000);
      Integer year = cal.get(Calendar.YEAR);
      year = year - (year % 3);
      for (String genre: ID2Genres.get(strs[1])) {
        context.write(new Text(year.toString()+","+genre), new Text(strs[1]+","+strs[2]));
      }
    }
    // We have written a java code where we have tokenized each word
    // and assigned them a hardcoded value equal to 1.
    // Eg: Dear 1, Bear 1,
  }

  public static class CounterReducer
          extends Reducer<Text,Text,Text,Text> {
    private static Map<String, String> ID2Names = new HashMap<>();
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String movieFileName = conf.get("movieNameFile");
      Path newPath = new Path(movieFileName);
      FileSystem fs = newPath.getFileSystem(conf);
      FSDataInputStream inputStream = fs.open(newPath);
      BufferedReader bufferedReader = new BufferedReader(
              new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      String line = new String();
      while ((line = bufferedReader.readLine()) != null) {
        String[] strs = line.toString().split(",", 2);
        ID2Names.put(strs[0], strs[1]);
      }
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
      Map<String, ArrayList<Float>> ID2Scores = new HashMap<>();
      for (Text value: values) {
        String[] strs = value.toString().split(",");
        String movieID = strs[0];
        if (!ID2Scores.containsKey(movieID)) ID2Scores.put(movieID, new ArrayList<Float>());
        ID2Scores.get(movieID).add(Float.parseFloat(strs[1]));
      }
      Float bestRating = Float.NEGATIVE_INFINITY;
      String bestMovie = new String();
      for (Map.Entry<String, ArrayList<Float>> entry: ID2Scores.entrySet()) {
        float sum = 0;
        for (float rating: entry.getValue()) {
          sum += rating;
        }
        sum /= entry.getValue().size();
        // TODO: Tie breaking!!!
        if (sum > bestRating || (sum == bestRating && ID2Names.get(entry.getKey()).compareTo(ID2Names.get(bestMovie)) < 0)) {
          bestRating = sum;
          bestMovie = entry.getKey();
        }
      }
      context.write(key, new Text(bestRating.toString()+","+bestMovie+","+ID2Names.get(bestMovie)));
    }
  }
  private static String ratingsFile;
  private static String genresFile;
  private static String outputScheme;
  private static String movieNameFile;
  public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("--ratingsFile")) {
        ratingsFile = args[++i];
      } else if (args[i].equals("--genresFile")) {
        genresFile = args[++i];
      } else if (args[i].equals("--outputScheme")) {
        outputScheme = args[++i];
      } else if (args[i].equals("--movieNameFile")) {
        movieNameFile = args[++i];
      } else {
        throw new IllegalArgumentException("Illegal cmd line arguement");
      }
    }

    if (ratingsFile == null || genresFile == null || outputScheme == null || movieNameFile == null) {
      throw new RuntimeException("Either outputpath or input path are not defined");
    }

    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


//    Job ID2GeneresJob = Job.getInstance(conf, "ID2GeneresJob");
//    ID2GeneresJob.setJarByClass(PopularMovies.class);
//    ID2GeneresJob.setNumReduceTasks(1);
//
//    ID2GeneresJob.setMapperClass(ID2GenresMapper.class);
//    ID2GeneresJob.setReducerClass(ID2GenresReducer.class);
//
//    // set mapper output key and value class
//    // if mapper and reducer output are the same types, you skip
//    ID2GeneresJob.setMapOutputKeyClass(Text.class);
//    ID2GeneresJob.setMapOutputValueClass(IntWritable.class);
//
//    // set reducer output key and value class
//    ID2GeneresJob.setOutputKeyClass(Text.class);
//    ID2GeneresJob.setOutputValueClass(IntWritable.class);
//
//    FileInputFormat.addInputPath(ID2GeneresJob, new Path(genresFile));
//    FileOutputFormat.setOutputPath(ID2GeneresJob, new Path(outputScheme+"1"));
//
//    ID2GeneresJob.waitForCompletion(true);
//
//    Job ID2NamesJob = Job.getInstance(conf, "ID2NamesJob");
//    ID2NamesJob.setJarByClass(PopularMovies.class);
//    ID2NamesJob.setNumReduceTasks(1);
//
//    ID2NamesJob.setMapperClass(ID2NamesMapper.class);
//    ID2NamesJob.setReducerClass(ID2NamesReducer.class);
//
//    // set mapper output key and value class
//    // if mapper and reducer output are the same types, you skip
//    ID2NamesJob.setMapOutputKeyClass(Text.class);
//    ID2NamesJob.setMapOutputValueClass(IntWritable.class);
//
//    // set reducer output key and value class
//    ID2NamesJob.setOutputKeyClass(Text.class);
//    ID2NamesJob.setOutputValueClass(IntWritable.class);
//
//    FileInputFormat.addInputPath(ID2NamesJob, new Path(movieNameFile));
//    FileOutputFormat.setOutputPath(ID2NamesJob, new Path(outputScheme+"2"));
//
//    ID2NamesJob.waitForCompletion(true);


    conf.set("genresFile", genresFile);
    conf.set("movieNameFile", movieNameFile);
    Job CounterJob = Job.getInstance(conf, "CounterJob");
    CounterJob.setJarByClass(PopularMovies.class);
    CounterJob.setNumReduceTasks(1);

    CounterJob.setMapperClass(CounterMapper.class);
    CounterJob.setReducerClass(CounterReducer.class);

    // set mapper output key and value class
    // if mapper and reducer output are the same types, you skip
    CounterJob.setMapOutputKeyClass(Text.class);
    CounterJob.setMapOutputValueClass(Text.class);

    // set reducer output key and value class
    CounterJob.setOutputKeyClass(Text.class);
    CounterJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(CounterJob, new Path(ratingsFile));
    FileOutputFormat.setOutputPath(CounterJob, new Path(outputScheme+"1"));

    CounterJob.waitForCompletion(true);
  }
}
