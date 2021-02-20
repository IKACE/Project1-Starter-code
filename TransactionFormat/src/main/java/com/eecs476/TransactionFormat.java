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

public class TransactionFormat {
  private final static IntWritable one = new IntWritable(1);
  private static Map<String, ArrayList<String>> ID2Genres = new HashMap<>();
  private static Map<Integer, HashMap<String, Integer>> GenresCounter = new HashMap<>();

  public static class TransMapper
          extends Mapper<LongWritable, Text, Text, Text>{

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
      context.write(new Text(strs[0]), new Text(strs[1]));
    }
    // We have written a java code where we have tokenized each word
    // and assigned them a hardcoded value equal to 1.
    // Eg: Dear 1, Bear 1,
  }

  public static class TransReducer
          extends Reducer<Text,Text,Text,Text> {

    // Input:
    // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
    // The value is a list of integers corresponding to each key: IntWritable
    // Eg: Bear, [1, 1],
    // Output:
    // The key is all the unique words present in the input text file: Text
    // The value is the number of occurrences of each of the unique words: IntWritable
    // Eg: Bear, 2; Car, 3,
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
      String IDSet = new String();
      for (Text val : values) {
        IDSet += val.toString();
        IDSet += ",";
      }
      IDSet = IDSet.substring(0, IDSet.length()-1);
      context.write(key, new Text(IDSet));
    }
  }
  private static String ratingsFile;
  private static String outputScheme;

  public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("--ratingsFile")) {
        ratingsFile = args[++i];
      } else if (args[i].equals("--outputScheme")) {
        outputScheme = args[++i];
      } else {
        throw new IllegalArgumentException("Illegal cmd line arguement");
      }
    }

    if (ratingsFile == null ||  outputScheme == null) {
      throw new RuntimeException("Either outputpath or input path are not defined");
    }

    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


    Job TransJob = Job.getInstance(conf, "TransJob");
    TransJob.setJarByClass(TransactionFormat.class);
    TransJob.setNumReduceTasks(1);

    TransJob.setMapperClass(TransMapper.class);
    TransJob.setReducerClass(TransReducer.class);

    // set mapper output key and value class
    // if mapper and reducer output are the same types, you skip
    TransJob.setMapOutputKeyClass(Text.class);
    TransJob.setMapOutputValueClass(Text.class);

    // set reducer output key and value class
    TransJob.setOutputKeyClass(Text.class);
    TransJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(TransJob, new Path(ratingsFile));
    FileOutputFormat.setOutputPath(TransJob, new Path(outputScheme));

    TransJob.waitForCompletion(true);
  }
}
