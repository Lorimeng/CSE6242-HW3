package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {
	public static class diffMapper 
	extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable in = new IntWritable(-1);
		private final static IntWritable out = new IntWritable(1);
		

		public void map (Object key, Text value, Context context) 
			throws IOException, InterruptedException{
				
				String[] data = value.toString().split("\t");
				int outNode = Integer.parseInt(data[0]);
				int inNode = Integer.parseInt(data[1]);
				context.write(new IntWritable(outNode), out);
				context.write(new IntWritable(inNode), in);
				
		}
	}

	public static class nodeMapper 
	extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable var = new IntWritable(1);
		public void map (Object key, Text value, Context context) 
			throws IOException, InterruptedException{
			String[] data = value.toString().split("\t");
			int count = Integer.parseInt(data[1]);
			context.write(new IntWritable(count), var);
		}

	}

	public static class AllReducer 
	extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum = sum + val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4");

    /* TODO: Needs to be implemented */

    job.setJarByClass(Q4.class);
    job.setMapperClass(diffMapper.class);
    job.setCombinerClass(AllReducer.class);
    job.setReducerClass(AllReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("output_middle"));
    job.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "job2");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(nodeMapper.class);
    job2.setCombinerClass(AllReducer.class);
    job2.setReducerClass(AllReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, new Path("output_middle"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
