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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.io.InputStreamReader;
import java.util.*;

public class FrequentItemsets {
    public static int s = 0;
    public static int k = 0;

    public static class SetMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited_str = value.toString().split(",");
            IntWritable one = new IntWritable(1);
            for (int i = 1; i < splited_str.length; i++) context.write(new Text(splited_str[i]), one);

        }
    }

    public static class SetReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int ss = Integer.parseInt(context.getConfiguration().get("s"));
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            if (sum >= ss) context.write(key, new IntWritable(1));
        }
    }

    public static class FrequentSetMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public static final IntWritable one = new IntWritable(1);
        public static Set<String> L1 = new HashSet<>();

        protected void setup(Context context) throws IOException {
            String outputName = context.getConfiguration().get("outputScheme");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path inFile = new Path(outputName + "1/part-r-00000");
            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
            }
            FSDataInputStream in = fs.open(inFile);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                String[] splited_strs = line.split(",");
                L1.add(splited_strs[0]);
            }
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited_str = value.toString().split(",");
            if (splited_str.length - 1 < k) return;
            ArrayList<String> temp = new ArrayList<>();
            for (int i = 1; i < splited_str.length; i++) {
                if (L1.contains(splited_str[i])) temp.add(splited_str[i]);
            }

            for (int i = 0; i < temp.size() - 1; i++) {
                for (int j = i + 1; j < temp.size(); j++) {
                    if (temp.get(i).compareTo(temp.get(j)) < 0) {
                        context.write(new Text(temp.get(i) + "," + temp.get(j)), one);
                    } else {
                        context.write(new Text(temp.get(j) + "," + temp.get(i)), one);
                    }
                }
            }

        }

    }

    public static class FrequentSetReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int ss = Integer.parseInt(context.getConfiguration().get("s"));
            int sum = 0;
            for (IntWritable v: values) sum += v.get();
            if (sum >= ss) context.write(key, new IntWritable(sum));
        }
    }


    private static String ratingsFilename;
    private static String outputScheme;
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-s")) {
                s = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--ratingsFile")) {
                ratingsFilename = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            }
            else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (ratingsFilename == null || outputScheme == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");
        conf.set("k", Integer.toString(k));
        conf.set("s", Integer.toString(s));
        conf.set("outputScheme", outputScheme);

        /* Job1: complete the mapping from movie id to genress */
        Job SetMapperJob = Job.getInstance(conf, "SetMapperJob");
        SetMapperJob.setJarByClass(FrequentItemsets.class);
        SetMapperJob.setNumReduceTasks(1);

        SetMapperJob.setMapperClass(SetMapper.class);
        SetMapperJob.setReducerClass(SetReducer.class);

        SetMapperJob.setMapOutputKeyClass(Text.class);
        SetMapperJob.setMapOutputValueClass(IntWritable.class);

        SetMapperJob.setOutputKeyClass(Text.class);
        SetMapperJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(SetMapperJob, new Path(ratingsFilename));
        FileOutputFormat.setOutputPath(SetMapperJob, new Path(outputScheme+"1"));

        SetMapperJob.waitForCompletion(true);

        /* Job2: for each year group, output the max genre and its count */
        Job FreqSetMapperJob = Job.getInstance(conf, "FreqSetMapperJob");
        FreqSetMapperJob.setJarByClass(FrequentItemsets.class);
        FreqSetMapperJob.setNumReduceTasks(1);

        FreqSetMapperJob.setMapperClass(FrequentSetMapper.class);
        FreqSetMapperJob.setReducerClass(FrequentSetReducer.class);

        FreqSetMapperJob.setMapOutputKeyClass(Text.class);
        FreqSetMapperJob.setMapOutputValueClass(IntWritable.class);

        FreqSetMapperJob.setOutputKeyClass(Text.class);
        FreqSetMapperJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(FreqSetMapperJob, new Path(ratingsFilename));
        FileOutputFormat.setOutputPath(FreqSetMapperJob, new Path(outputScheme+"2"));

        FreqSetMapperJob.waitForCompletion(true);

    }
}
