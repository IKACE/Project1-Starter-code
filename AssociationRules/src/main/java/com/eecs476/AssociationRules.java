package com.eecs476;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class AssociationRules {

    private final static IntWritable one = new IntWritable(1);
    private final static ArrayList<String> Pass1Counter = new ArrayList<>();
    private static Set<SortedSet<String>> FrequentSet = new HashSet<>();
    private final static HashMap<String, Integer> Pass1Map = new HashMap<>();
    private static HashMap<SortedSet<String>, Integer> FreqSetMap = new HashMap<>();
    public static int k = -1;
    public static int s = -1;

    public static class Pass1Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            for (int i=1; i<strs.length; i++) {
                context.write(new Text(strs[i]), one);
            }
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class Pass1Reducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

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
            for (IntWritable value: values) {
                sum += value.get();
            }
            if (sum >= s) {
                Pass1Counter.add(key.toString());
                Pass1Map.put(key.toString(), sum);
                context.write(key, new IntWritable(sum));
            }
        }
        // generate candidate set
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i=0; i<Pass1Counter.size(); i++) {
                for (int j=i+1; j<Pass1Counter.size(); j++) {
                    SortedSet<String> pair = new TreeSet<>();
                    pair.add(Pass1Counter.get(i));
                    pair.add(Pass1Counter.get(j));
                    FrequentSet.add(pair);
                }
            }
        }
    }

    // TODO:: To extends the solution into k > 2 and edge condition k = 1

    public static class Pass2Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            Set<String> movies = new HashSet<>();
            for (int i=1; i<strs.length; i++) {
                movies.add(strs[i]);
            }
            for (SortedSet<String> pair: FrequentSet) {
                String keyStr = new String();
                boolean containsAll = true;
                for (String cand: pair) {
                    keyStr += cand;
                    keyStr += ",";
                    if (!movies.contains(cand)) {
                        containsAll = false;
                        break;
                    }
                }
                if (containsAll) {
                    keyStr = keyStr.substring(0, keyStr.length()-1);
                    context.write(new Text(keyStr), one);
                }
            }
//            for (int i=1; i<strs.length; i++) {
//                for (int j=i+1; j<strs.length; j++) {
//                    SortedSet<String> pair = new TreeSet<>();
//                    if (Pass1Counter.contains(strs[i]) && Pass1Counter.contains(strs[j])) {
//                        pair.add(strs[i]);
//                        pair.add(strs[j]);
//                        String keyStr = new String();
//                        for (String movie: pair) {
//                            keyStr += movie;
//                            keyStr += ",";
//                        }
//                        keyStr = keyStr.substring(0, keyStr.length()-1);
//                        context.write(new Text(keyStr), one);
//                    }
//                }
//            }


//            for (SortedSet<String> pair: pairs) {
//                String keyStr = new String();
//                for (String movie: pair) {
//                    keyStr += movie;
//                    keyStr += ",";
//                }
//                keyStr = keyStr.substring(0, keyStr.length()-1);
//                context.write(new Text(keyStr), one);
//            }
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class Pass2Reducer
            extends Reducer<Text,IntWritable,Text, DoubleWritable> {

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
            for (IntWritable value: values) {
                sum += value.get();
            }
            if (sum >= s) {
                // TODO: free k=1 set, fill in k=2 set
//                String[] strs = key.toString().split(",");
//                SortedSet<String> pair = new TreeSet<>();
//                pair.add(strs[0]);
//                pair.add(strs[1]);
//                FrequentSet.add(pair);
                String[] strs = key.toString().split(",");
                SortedSet<String> victim = new TreeSet<>();
                for (int i=0; i<strs.length; i++) {
                    victim.add(strs[i]);
                }
                FreqSetMap.put(victim, sum);
            } else {
                String[] strs = key.toString().split(",");
                SortedSet<String> victim = new TreeSet<>();
                for (int i=0; i<strs.length; i++) {
                    victim.add(strs[i]);
                }
                FrequentSet.remove(victim);
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<SortedSet<String>, Integer> entry: FreqSetMap.entrySet()) {
                Iterator<String> itr = entry.getKey().iterator();
                String left = itr.next();
                String right = itr.next();
                context.write(new Text(left+"->"+right), new DoubleWritable(entry.getValue()/(double)Pass1Map.get(left)));
                context.write(new Text(right+"->"+left), new DoubleWritable(entry.getValue()/(double)Pass1Map.get(right)));
            }
        }
//        // generate new cand set
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            Set<SortedSet<String>> newFreqSet = new HashSet<>();
//            for (SortedSet<String> pair: FrequentSet) {
//                for (String cand: Pass1Counter) {
//                    if (pair.contains(cand)) continue;
//                    SortedSet<String> newPair = new TreeSet<>(pair);
//                    newPair.add(cand);
//                    newFreqSet.add(newPair);
//                }
//            }
//            FrequentSet = new HashSet<>(newFreqSet);
//            for (SortedSet<String> pair: FrequentSet) {
////                String keyStr = new String();
////                for (String cand: pair) {
////                    keyStr += cand;
////                    keyStr += ",";
////                }
////                keyStr = keyStr.substring(0, keyStr.length()-1);
//                System.out.println("In NewFreqSet:"+ pair);
//            }
//        }
    }

//    public static class PassKMapper
//            extends Mapper<LongWritable, Text, Text, IntWritable> {
//
//        public void map(LongWritable key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            String[] strs = value.toString().split(",");
//            for (int i=1; i<strs.length; i++) {
//                for (int j=i+1; j<strs.length; j++) {
//                    SortedSet<String> pair = new TreeSet<>();
//                    if (Pass1Counter.containsKey(strs[i]) && Pass1Counter.containsKey(strs[j])) {
//                        pair.add(strs[i]);
//                        pair.add(strs[j]);
//                        String keyStr = new String();
//                        for (String movie: pair) {
//                            keyStr += movie;
//                            keyStr += ",";
//                        }
//                        keyStr = keyStr.substring(0, keyStr.length()-1);
//                        context.write(new Text(keyStr), one);
//                    }
//                }
//            }
//
////            for (SortedSet<String> pair: pairs) {
////                String keyStr = new String();
////                for (String movie: pair) {
////                    keyStr += movie;
////                    keyStr += ",";
////                }
////                keyStr = keyStr.substring(0, keyStr.length()-1);
////                context.write(new Text(keyStr), one);
////            }
//        }
//        // We have written a java code where we have tokenized each word
//        // and assigned them a hardcoded value equal to 1.
//        // Eg: Dear 1, Bear 1,
//    }
//
//    public static class PassKReducer
//            extends Reducer<Text,IntWritable,Text,IntWritable> {
//
//        // Input:
//        // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
//        // The value is a list of integers corresponding to each key: IntWritable
//        // Eg: Bear, [1, 1],
//        // Output:
//        // The key is all the unique words present in the input text file: Text
//        // The value is the number of occurrences of each of the unique words: IntWritable
//        // Eg: Bear, 2; Car, 3,
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable value: values) {
//                sum += value.get();
//            }
//            if (sum >= s) {
//                // TODO: free k=1 set, fill in k=2 set
//                Pass1Counter.clear();
//                String[] strs = key.toString().split(",");
//                SortedSet<String> pair = new TreeSet<>();
//                pair.add(strs[0]);
//                pair.add(strs[1]);
//                FrequentSet.add(pair);
//                context.write(key, new IntWritable(sum));
//            }
//        }
//    }

    private static String ratingsFile;
    private static String outputScheme;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("-s")) {
                s = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (ratingsFile == null || outputScheme == null || s == -1 || k == -1) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes

        Job Pass1Job = Job.getInstance(conf, "Pass1Job");
        Pass1Job.setJarByClass(AssociationRules.class);
        Pass1Job.setNumReduceTasks(1);

        Pass1Job.setMapperClass(Pass1Mapper.class);
        Pass1Job.setReducerClass(Pass1Reducer.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        Pass1Job.setMapOutputKeyClass(Text.class);
        Pass1Job.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        Pass1Job.setOutputKeyClass(Text.class);
        Pass1Job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(Pass1Job, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(Pass1Job, new Path(outputScheme + "1"));

        Pass1Job.waitForCompletion(true);


        Job Pass2Job = Job.getInstance(conf, "Pass2Job");
        Pass2Job.setJarByClass(AssociationRules.class);
        Pass2Job.setNumReduceTasks(1);

        Pass2Job.setMapperClass(Pass2Mapper.class);
        Pass2Job.setReducerClass(Pass2Reducer.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        Pass2Job.setMapOutputKeyClass(Text.class);
        Pass2Job.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        Pass2Job.setOutputKeyClass(Text.class);
        Pass2Job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(Pass2Job, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(Pass2Job, new Path(outputScheme + "2"));

        Pass2Job.waitForCompletion(true);
    }
}
