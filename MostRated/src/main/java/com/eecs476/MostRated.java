package com.eecs476;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class MostRated{
  //TODO: Finish Part1 Q1 here.
  private final static IntWritable one = new IntWritable(1);
  private static Map<String, ArrayList<String>> ID2Genres = new HashMap<>();
  private static Map<Integer, HashMap<String, Integer>> GenresCounter = new HashMap<>();
  public static class ID2GenresMapper
          extends Mapper<LongWritable, Text, Text, IntWritable>{

    // Output:
    // We have the hardcoded value in our case which is 1: IntWritable
    // The key is the tokenized words: Text
    private Text word = new Text();

    // Input:
    // We define the data types of input and output key/value pair after the class declaration using angle brackets.
    // Both the input and output of the Mapper is a key/value pair.

    // The key is nothing but the offset of each line in the text file: LongWritable
    // The value is each individual line (as shown in the figure at the right): Text
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] strs = value.toString().split(",");
      ID2Genres.put(strs[0], new ArrayList<>());
      for (int i=1; i<strs.length; i++) {
        ID2Genres.get(strs[0]).add(strs[i]);
      }
    }
    // We have written a java code where we have tokenized each word
    // and assigned them a hardcoded value equal to 1.
    // Eg: Dear 1, Bear 1,
  }

  public static class ID2GenresReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    // Input:
    // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
    // The value is a list of integers corresponding to each key: IntWritable
    // Eg: Bear, [1, 1],
    // Output:
    // The key is all the unique words present in the input text file: Text
    // The value is the number of occurrences of each of the unique words: IntWritable
    // Eg: Bear, 2; Car, 3,

  }
  public static class CounterMapper
          extends Mapper<LongWritable, Text, Text, IntWritable>{

    // Output:
    // We have the hardcoded value in our case which is 1: IntWritable
    // The key is the tokenized words: Text
    private Text word = new Text();

    // Input:
    // We define the data types of input and output key/value pair after the class declaration using angle brackets.
    // Both the input and output of the Mapper is a key/value pair.

    // The key is nothing but the offset of each line in the text file: LongWritable
    // The value is each individual line (as shown in the figure at the right): Text
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] strs = value.toString().split(",");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(Long.parseLong(strs[3]) * 1000);
      Integer year = cal.get(Calendar.YEAR);
      year = year - (year % 3);
      for (String genre: ID2Genres.get(strs[1])) {
        context.write(new Text(genre+","+year.toString()), one);
      }
    }
    // We have written a java code where we have tokenized each word
    // and assigned them a hardcoded value equal to 1.
    // Eg: Dear 1, Bear 1,
  }

  public static class CounterReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    // Input:
    // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
    // The value is a list of integers corresponding to each key: IntWritable
    // Eg: Bear, [1, 1],
    // Output:
    // The key is all the unique words present in the input text file: Text
    // The value is the number of occurrences of each of the unique words: IntWritable
    // Eg: Bear, 2; Car, 3,
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  private static String ratingsFile;
  private static String genresFile;
  private static String outputScheme;

  public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("--ratingsFile")) {
        ratingsFile = args[++i];
      } else if (args[i].equals("--genresFile")) {
        genresFile = args[++i];
      } else if (args[i].equals("--outputScheme")) {
        outputScheme = args[++i];
      } else {
        throw new IllegalArgumentException("Illegal cmd line arguement");
      }
    }

    if (ratingsFile == null || genresFile == null || outputScheme == null) {
      throw new RuntimeException("Either outputpath or input path are not defined");
    }

    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


    Job ID2GeneresJob = Job.getInstance(conf, "ID2GeneresJob");
    ID2GeneresJob.setJarByClass(MostRated.class);
    ID2GeneresJob.setNumReduceTasks(1);

    ID2GeneresJob.setMapperClass(ID2GenresMapper.class);
    ID2GeneresJob.setReducerClass(ID2GenresReducer.class);

    // set mapper output key and value class
    // if mapper and reducer output are the same types, you skip
    ID2GeneresJob.setMapOutputKeyClass(Text.class);
    ID2GeneresJob.setMapOutputValueClass(IntWritable.class);

    // set reducer output key and value class
    ID2GeneresJob.setOutputKeyClass(Text.class);
    ID2GeneresJob.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(ID2GeneresJob, new Path(genresFile));
    FileOutputFormat.setOutputPath(ID2GeneresJob, new Path(outputScheme+"1"));

    ID2GeneresJob.waitForCompletion(true);

    Job CounterJob = Job.getInstance(conf, "CounterJob");
    CounterJob.setJarByClass(MostRated.class);
    CounterJob.setNumReduceTasks(1);

    CounterJob.setMapperClass(CounterMapper.class);
    CounterJob.setReducerClass(CounterReducer.class);

    // set mapper output key and value class
    // if mapper and reducer output are the same types, you skip
    CounterJob.setMapOutputKeyClass(Text.class);
    CounterJob.setMapOutputValueClass(IntWritable.class);

    // set reducer output key and value class
    CounterJob.setOutputKeyClass(Text.class);
    CounterJob.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(CounterJob, new Path(ratingsFile));
    FileOutputFormat.setOutputPath(CounterJob, new Path(outputScheme+"2"));

    CounterJob.waitForCompletion(true);
  }

}
